import json
import logging
import os
import tempfile
import time
import uuid
from typing import List, Dict, Any

import azure.functions as func
import cv2
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.eventhub import EventHubProducerClient, EventData

def main(myblob: func.InputStream):
    """Azure Function triggered by blob creation to preprocess videos
    
    Args:
        myblob (func.InputStream): The input blob that triggered the function
    """
    logging.info(f"Python blob trigger function processed blob: {myblob.name}")
    
    try:
        # Extract video information
        blob_name = myblob.name
        container_name = os.environ.get('RAW_VIDEOS_CONTAINER', 'raw-footage')
        video_id = str(uuid.uuid4())
        
        logging.info(f"Processing video {video_id}: {blob_name} from {container_name}")
        
        # Save blob content to temporary file
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_file:
            temp_file.write(myblob.read())
            temp_file_path = temp_file.name
        
        try:
            # Initialize blob storage client
            blob_service_client = BlobServiceClient.from_connection_string(
                os.environ["AZURE_STORAGE_CONNECTION_STRING"]
            )
            
            # Get destination container
            destination_container_name = os.environ.get('PROCESSED_SEGMENTS_CONTAINER', 'processed-videos')
            destination_container_client = blob_service_client.get_container_client(destination_container_name)
            #destination_container_client.create_container_if_not_exists()

            if not destination_container_client.exists():
                destination_container_client.create_container()
            
            # Split video into segments
            segments = split_video_into_segments(
                temp_file_path,
                int(os.environ.get('SEGMENT_DURATION_SECONDS', '120')),
                video_id
            )
            
            # Upload segments
            segment_metadata = []
            for segment_path, segment_number, start_frame in segments:
                # Extract the filename from the path
                segment_filename = os.path.basename(segment_path)
                
                # Upload segment
                with open(segment_path, 'rb') as segment_file:
                    blob_client = destination_container_client.get_blob_client(segment_filename)
                    blob_client.upload_blob(
                        segment_file,
                        overwrite=True,
                        content_settings=ContentSettings(content_type='video/mp4')
                    )
                
                # Add to metadata list
                segment_metadata.append({
                    'segment_id': str(uuid.uuid4()),
                    'video_id': video_id,
                    'segment_number': segment_number,
                    'start_frame': start_frame,
                    'blob_name': segment_filename,
                    'container_name': destination_container_name,
                    'start_time': (segment_number - 1) * int(os.environ.get('SEGMENT_DURATION_SECONDS', '120')),
                    'duration': int(os.environ.get('SEGMENT_DURATION_SECONDS', '120')),
                    'url': f"https://{blob_service_client.account_name}.blob.core.windows.net/{destination_container_name}/{segment_filename}"
                })
                
                # Clean up segment file
                os.unlink(segment_path)
            
            # Send segments metadata to Event Hub
            send_to_event_hub(segment_metadata)
            
            logging.info(f"Successfully processed video {video_id} into {len(segments)} segments")
            
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)
            
    except Exception as e:
        logging.error(f"Error processing video: {str(e)}")
        raise



def split_video_into_segments(video_path: str, segment_duration_seconds: int, video_id: str) -> List[tuple]:
    """Split a video into segments of specified duration
    
    Args:
        video_path (str): Path to the video file
        segment_duration_seconds (int): Duration of each segment in seconds
        video_id (str): ID of the original video
        
    Returns:
        List[tuple]: List of (segment_path, segment_number, start_frame) tuples
    """
    logging.info(f"Splitting video into {segment_duration_seconds}-second segments")
    
    # Open the video
    cap = cv2.VideoCapture(video_path)
    
    # Check if video opened successfully
    if not cap.isOpened():
        raise Exception("Error opening video file")
    
    # Get video properties
    fps = cap.get(cv2.CAP_PROP_FPS)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    
    logging.info(f"Video properties: {fps} fps, {width}x{height}, {total_frames} frames")
    
    # Calculate frames per segment
    frames_per_segment = int(fps * segment_duration_seconds)
    
    # Create temporary directory for segments
    temp_dir = tempfile.mkdtemp()
    
    segments = []
    segment_number = 1
    current_segment_path = None
    out = None
    frame_count = 0
    
    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            
            # Start a new segment if needed
            if frame_count % frames_per_segment == 0:
                # Close previous writer if exists
                if out is not None:
                    out.release()
                
                # Create new output file with frame number in filename
                segment_start_frame = frame_count
                current_segment_path = os.path.join(
                    temp_dir, 
                    f"{video_id}_{segment_start_frame:06d}.mp4"
                )
                
                # Create VideoWriter object
                fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # codec
                out = cv2.VideoWriter(current_segment_path, fourcc, fps, (width, height))
                
                # Add to segments list with start frame information
                segments.append((current_segment_path, segment_number, segment_start_frame))
                segment_number += 1
            
            # Write the frame
            out.write(frame)
            frame_count += 1
    
    finally:
        # Release resources
        if out is not None:
            out.release()
        cap.release()
    
    logging.info(f"Created {len(segments)} video segments")
    return segments


def send_to_event_hub(segment_metadata: List[Dict[str, Any]]) -> None:
    """Send segment metadata to Event Hub
    
    Args:
        segment_metadata (List[Dict[str, Any]]): List of segment metadata
    """
    if not segment_metadata:
        return
    
    # Get Event Hub configuration
    event_hub_connection_string = os.environ.get('EVENT_HUB_CONNECTION_STRING')
    event_hub_name = os.environ.get('EVENT_HUB_NAME', 'video-segments')
    
    if not event_hub_connection_string or not event_hub_name:
        logging.warning("Event Hub not configured, skipping event publishing")
        return
    
    try:
        # Create Event Hub producer
        producer = EventHubProducerClient.from_connection_string(
            conn_str=event_hub_connection_string,
            eventhub_name=event_hub_name
        )
        
        # Send events
        with producer:
            # Create a batch
            event_data_batch = producer.create_batch()
            
            for segment in segment_metadata:
                # Convert segment metadata to JSON
                segment_json = json.dumps(segment)
                
                # Create event data
                event_data = EventData(segment_json)
                
                # Add properties for filtering
                event_data.properties = {
                    'video_id': segment['video_id'],
                    'segment_number': segment['segment_number']
                }
                
                # Check if the batch is full
                if not event_data_batch.add(event_data):
                    # If batch is full, send it and create a new one
                    producer.send_batch(event_data_batch)
                    event_data_batch = producer.create_batch()
                    event_data_batch.add(event_data)
            
            # Send any remaining events
            if len(event_data_batch) > 0:
                producer.send_batch(event_data_batch)
                
        logging.info(f"Sent {len(segment_metadata)} events to Event Hub")
        
    except Exception as e:
        logging.error(f"Error sending to Event Hub: {str(e)}")