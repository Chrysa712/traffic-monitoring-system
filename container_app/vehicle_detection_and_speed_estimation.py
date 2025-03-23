import os
import cv2
import json
import logging
import psycopg2
import pandas as pd
from ultralytics import YOLO
from psycopg2.extras import execute_values
from azure.eventhub import EventHubConsumerClient, EventHubProducerClient, EventData
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime, timezone, timedelta
from tracker import Tracker

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


FRAME_PROCESSING_INTERVAL = 2
OFFSET_RANGE = 15
DISTANCE_BETWEEN_LINES_METERS = 20
SPEED_LIMIT_KMH = 130
SECONDS_PER_MINUTE = 60
MINUTES_PER_BIN = 5

TOP_LINE_Y = 375
TOP_LINE_X_START = 380
TOP_LINE_X_END = 873
BOTTOM_LINE_Y = 470
BOTTOM_LINE_X_START = 227
BOTTOM_LINE_X_END = 1023

RED_COLOR = (0, 0, 255)
BLUE_COLOR = (255, 0, 0)
GREEN_COLOR = (0, 255, 0)
BLACK_COLOR = (0, 0, 0)


def extract_from_url(url):
    """
    Extract video ID and starting frame from the video URL.
    
    Args:
        url (str): URL to the video (e.g., https://storageaccount.blob.core.windows.net/container/74a8d50b-1234-5678-abcd-1234abcd5678_24000.mp4)
    
    Returns:
        tuple: (video_id, starting_frame)
    """
    segment_filename = url.split('/')[-1]
    parts = segment_filename.split('_')
    video_id = parts[0]
    starting_frame = int(parts[-1].split('.')[0])
    return video_id, starting_frame


def authorize_segment_url(url):
    """
    Authorize the video URL with a SAS token.
    
    Args:
        url (str): URL to the video
    
    Returns:
        str: Authorized URL with a SAS token
    """
    url_parts = url.split('/')
    storage_account = url_parts[2].split('.')[0]
    container_name = url_parts[3]
    blob_name = '/'.join(url_parts[4:])

    sas_token = generate_blob_sas(
        account_name=storage_account,
        container_name=container_name,
        blob_name=blob_name,
        account_key=os.getenv("STORAGE_ACCOUNT_KEY"),
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(minutes=10)
    )

    return f"{url}?{sas_token}"


def calculate_five_min_bin(absolute_frame, fps):
    """
    Calculate which 5-minute bin the frame belongs to.
    
    Args:
        absolute_frame (int): The absolute frame number with respect to the original video
        fps (float): Frames per second of the video
    
    Returns:
        int: The bin number (starting from 1)
    """
    frames_per_bin = int(fps * SECONDS_PER_MINUTE * MINUTES_PER_BIN)
    return (absolute_frame // frames_per_bin) + 1


def is_crossing_line(cy, line_y):
    """
    Check if a point is crossing a horizontal line within the offset range.
    
    Args:
        cy (int): Y-coordinate of the detected object center
        line_y (int): Y-coordinate of the line
    
    Returns:
        bool: True if the point is crossing the line, False otherwise
    """
    return line_y - OFFSET_RANGE < cy < line_y + OFFSET_RANGE


def calculate_speed(elapsed_frames, fps):
    """
    Calculate speed in km/h based on elapsed frames.
    
    Args:
        elapsed_frames (int): Number of frames between two points
        fps (float): Frames per second of the video
    
    Returns:
        float: Speed in km/h
    """
    elapsed_time_s = elapsed_frames / fps
    speed_mps = DISTANCE_BETWEEN_LINES_METERS / elapsed_time_s
    return speed_mps * 3.6  # Convert m/s to km/h


def draw_visualization(frame, num_of_vehicles_going_up, num_of_vehicles_going_down):
    """
    Add visualization elements to the frame.
    
    Args:
        frame: The video frame to draw on
        num_of_vehicles_going_up (int): The number of vehicles going up
        num_of_vehicles_going_down (int): The number of vehicles going down
    """
    cv2.line(frame, (TOP_LINE_X_START, TOP_LINE_Y), (TOP_LINE_X_END, TOP_LINE_Y), BLUE_COLOR, thickness=3)
    cv2.line(frame, (BOTTOM_LINE_X_START, BOTTOM_LINE_Y), (BOTTOM_LINE_X_END, BOTTOM_LINE_Y), BLUE_COLOR, thickness=3)

    cv2.putText(frame, f'Going Up - {num_of_vehicles_going_up}', (10, 60), 
               cv2.FONT_HERSHEY_SIMPLEX, 0.5, BLACK_COLOR, 1, cv2.LINE_AA)
    cv2.putText(frame, f'Going Down - {num_of_vehicles_going_down}', (10, 30), 
               cv2.FONT_HERSHEY_SIMPLEX, 0.5, BLACK_COLOR, 1, cv2.LINE_AA)


def send_speeding_alert(vehicle_id, vehicle_type, direction, speed_kmh, absolute_frame, video_id, alert_producer):
    """
    Send a speeding alert to the alerts Event Hub.
    
    Args:
        vehicle_id (uuid): ID of the vehicle
        vehicle_type (str): Type of vehicle (CAR or TRUCK)
        direction (str): Direction of travel (UP or DOWN)
        speed_kmh (float): Speed in km/h
        absolute_frame (int): Absolute frame number with respect to the original video
        video_id (str): Identifier for the video source
        alert_producer (EventHubProducerClient): Client for sending speeding alerts to Event Hub
    """
    alert_data = {
        "vehicle_id": vehicle_id,
        "vehicle_type": vehicle_type,
        "direction": direction,
        "speed_kmh": speed_kmh,
        "speed_limit_kmh": SPEED_LIMIT_KMH,
        "excess_speed_kmh": speed_kmh - SPEED_LIMIT_KMH,
        "absolute_frame": absolute_frame,
        "video_id": video_id
    }
    alert_json = json.dumps(alert_data)

    event_data_batch = alert_producer.create_batch()
    event_data_batch.add(EventData(alert_json))
    alert_producer.send_batch(event_data_batch)

    logger.info(f"SPEED ALERT: {vehicle_type} {vehicle_id} - Lane {direction} - Speed {speed_kmh:.2f} km/h - Excess: {speed_kmh - SPEED_LIMIT_KMH:.2f} km/h")


def process_vehicle(bbox_id, vehicle_type, absolute_frame, fps, video_id, crossed_top_line, processed_up, 
                    crossed_bottom_line, processed_down, events, alert_producer, frame, visualize=True):
    """
    Process a single tracked vehicle and produce an event if it crossed the lines.
    
    Args:
        bbox_id: Bounding box and ID of the vehicle
        vehicle_type (str): Type of vehicle (CAR or TRUCK)
        absolute_frame (int): Absolute frame number with respect to the original video
        fps (float): Frames per second of the video
        video_id (str): Identifier for the video source
        crossed_top_line (dict): Dictionary tracking vehicles crossing the top line
        processed_up (set): Set of vehicle IDs going up
        crossed_bottom_line (dict): Dictionary tracking vehicles crossing the bottom line
        processed_down (set): Set of vehicle IDs going down
        events (list): List to append the produced events to
        alert_producer (EventHubProducerClient): Client for sending speeding alerts to Event Hub
        frame: The video frame to draw on
        visualize (bool): Whether to draw visualizations
    """
    x1, y1, x2, y2, obj_id = bbox_id
    cx = (x1 + x2) // 2
    cy = (y1 + y2) // 2

    color = GREEN_COLOR if vehicle_type == "TRUCK" else RED_COLOR

    five_min_bin = calculate_five_min_bin(absolute_frame, fps)

    # Going UP detection
    if obj_id not in crossed_bottom_line and is_crossing_line(cy, BOTTOM_LINE_Y):
        crossed_bottom_line[obj_id] = absolute_frame

    if obj_id in crossed_bottom_line and obj_id not in processed_up and is_crossing_line(cy, TOP_LINE_Y):
        processed_up.add(obj_id)
        speed_kmh = calculate_speed(absolute_frame - crossed_bottom_line[obj_id], fps)

        if speed_kmh > SPEED_LIMIT_KMH:
            send_speeding_alert(obj_id, vehicle_type, "UP", speed_kmh, absolute_frame, video_id, alert_producer)

        events.append((obj_id, vehicle_type, "UP", speed_kmh, five_min_bin, video_id))
        logger.info(f'EVENT: {vehicle_type} {obj_id} - Lane UP - Speed {speed_kmh:.2f} km/h - Bin {five_min_bin}')

        if visualize:
            cv2.circle(frame, (cx, cy), 4, color, -1)
            cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

    # Going DOWN detection
    if obj_id not in crossed_top_line and is_crossing_line(cy, TOP_LINE_Y):
        crossed_top_line[obj_id] = absolute_frame

    if obj_id in crossed_top_line and obj_id not in processed_down and is_crossing_line(cy, BOTTOM_LINE_Y):
        processed_down.add(obj_id)
        speed_kmh = calculate_speed(absolute_frame - crossed_top_line[obj_id], fps)

        if speed_kmh > SPEED_LIMIT_KMH:
            send_speeding_alert(obj_id, vehicle_type, "DOWN", speed_kmh, absolute_frame, video_id, alert_producer)

        events.append((obj_id, vehicle_type, "DOWN", speed_kmh, five_min_bin, video_id))
        logger.info(f'EVENT: {vehicle_type} {obj_id} - Lane DOWN - Speed {speed_kmh:.2f} km/h - Bin {five_min_bin}')

        if visualize:
            cv2.circle(frame, (cx, cy), 4, color, -1)
            cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)


def process_video_segment(video_url, visualize=False):
    """
    Process a video from a URL and generate events for detected vehicles.
    
    Args:
        video_url (str): URL to the video
        visualize (bool): Whether to show visualization windows
    
    Returns:
        list: List of JSON event strings
    """
    alert_producer = EventHubProducerClient.from_connection_string(
        conn_str=os.getenv("EVENT_HUB_CONNECTION_STRING"),
        eventhub_name=os.getenv("ALERTS_EVENT_HUB_NAME")
    )

    video_id, starting_frame = extract_from_url(video_url)
    logger.info(f"Processing video segment: {video_url}")
    logger.info(f"Video ID: {video_id}, Starting Frame: {starting_frame}")

    model = YOLO('yolov8s.pt')
    vehicle_classes = [2, 7]  # COCO dataset class indices for car and truck
    class_names = {2: "car", 7: "truck"}

    video_url_with_sas = authorize_segment_url(video_url)

    cap = cv2.VideoCapture(video_url_with_sas)
    if not cap.isOpened():
        raise ValueError(f"Could not open video file: {video_url}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    if fps == 0:
        raise ValueError("Could not get the frame rate of the video.")
    logger.info(f"Video frame rate: {fps} fps")

    car_tracker = Tracker()
    truck_tracker = Tracker()

    crossed_top_line = {}
    crossed_bottom_line = {}
    processed_up = set()
    processed_down = set()

    events = []
    segment_frame_count = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break

        segment_frame_count += 1

        # Absolute frame is the frame number with respect to the original video
        absolute_frame = starting_frame + segment_frame_count

        # Detect vehicles
        results = model.predict(frame, classes=vehicle_classes)
        detections = results[0].boxes.data.numpy()
        detections_df = pd.DataFrame(detections).astype('float')

        cars_rect = []
        trucks_rect = []
        for _, row in detections_df.iterrows():
            x1, y1, x2, y2 = int(row[0]), int(row[1]), int(row[2]), int(row[3])
            detected_class = int(row[5])

            if class_names[detected_class] == 'car':
                cars_rect.append([x1, y1, x2, y2])
            elif class_names[detected_class] == 'truck':
                trucks_rect.append([x1, y1, x2, y2])

        cars_bboxes_ids = car_tracker.update(cars_rect)
        for car_bbox_id in cars_bboxes_ids:
            process_vehicle(car_bbox_id, "CAR", absolute_frame, fps, video_id, crossed_top_line, processed_up, 
                            crossed_bottom_line, processed_down, events, alert_producer, frame, visualize)

        trucks_bboxes_ids = truck_tracker.update(trucks_rect)
        for truck_bbox_id in trucks_bboxes_ids:
            process_vehicle(truck_bbox_id, "TRUCK", absolute_frame, fps, video_id, crossed_top_line, processed_up, 
                            crossed_bottom_line, processed_down, events, alert_producer, frame, visualize)

        if visualize:
            draw_visualization(frame, len(processed_up), len(processed_down))
            cv2.imshow('Vehicle Tracking', frame)

            # Exit on 'q' press
            if cv2.waitKey(15) & 0xFF == ord('q'):
                break

    cap.release()
    if visualize:
        cv2.destroyAllWindows()

    alert_producer.close()

    return events


DB_CONFIG = {
    "host": os.getenv("COSMOS_POSTGRES_HOST"),
    "port": os.getenv("COSMOS_POSTGRES_PORT"),
    "database": os.getenv("COSMOS_POSTGRES_DB"),
    "user": os.getenv("COSMOS_POSTGRES_USER"),
    "password": os.getenv("COSMOS_POSTGRES_PASSWORD")
}

INSERT_SQL = """
    INSERT INTO vehicle_events (vehicle_id, vehicle_type, lane, speed_kmh, five_min_bin, video_id)
    VALUES %s;
"""

def batch_insert_vehicle_events(events):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, events)
            logger.info("Batch inserted successfully.")


def on_event(partition_context, event):
    event_body_str = event.body_as_str()
    event_data = json.loads(event_body_str)
    segment_url = event_data.get("segment_url")
    logger.info(f"Segment URL: {segment_url}")

    events = process_video_segment(segment_url, visualize=False)

    batch_insert_vehicle_events(events)

    logger.info(f"Total events generated: {len(events)}")
    if events:
        logger.info(f"First event: {events[0]}")
        logger.info(f"Last event: {events[-1]}")

    # Acknowledge the event
    partition_context.update_checkpoint(event)


def main():
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        conn_str=os.getenv("STORAGE_CONNECTION_STRING"),
        container_name=os.getenv("BLOB_CONTAINER_NAME")
    )

    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=os.getenv("EVENT_HUB_CONNECTION_STRING"),
        eventhub_name=os.getenv("SEGMENTS_EVENT_HUB_NAME"),
        consumer_group=os.getenv("DEFAULT_CONSUMER_GROUP"),
        checkpoint_store=checkpoint_store
    )

    with consumer:
        logger.info("Receiving events...")
        consumer.receive(
            on_event=on_event,
            starting_position="-1"  # "-1" is from the beginning of the partition - Ignored if there is a checkpoint
        )


if __name__ == "__main__":
    main()
