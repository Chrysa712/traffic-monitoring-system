import uuid
import math


class Tracker:
    def __init__(self, max_distance_pixels=35):
        """
        Initialize the detected objects tracker.
        
        Args:
            max_distance (int): Maximum distance in pixels between detected object centers 
                                for them to be considered the same object across frames
        """
        self.center_points = {}
        self.max_distance_pixels = max_distance_pixels


    def update(self, objects_bboxes):
        """
        Update the tracker with new detections.
        
        Args:
            objects_bboxes (list): List of [x1, y1, x2, y2] coordinates of the detected objects bounding boxes
        
        Returns:
            list: List of [x1, y1, x2, y2, object_id] for each tracked object
        """
        objects_bboxes_ids = []
        for bbox in objects_bboxes:
            x1, y1, x2, y2 = bbox
            cx = (x1 + x2) // 2
            cy = (y1 + y2) // 2

            same_object_detected = False
            for obj_id, center in self.center_points.items():
                dist = math.hypot(cx - center[0], cy - center[1])
                if dist <= self.max_distance_pixels:
                    self.center_points[obj_id] = (cx, cy)
                    objects_bboxes_ids.append([x1, y1, x2, y2, obj_id])
                    same_object_detected = True
                    break

            if not same_object_detected:
                new_id = str(uuid.uuid4())
                self.center_points[new_id] = (cx, cy)
                objects_bboxes_ids.append([x1, y1, x2, y2, new_id])

        # Clean the dictionary from objects not detected anymore
        new_center_points = {}
        for obj_bbox_id in objects_bboxes_ids:
            _, _, _, _, obj_id = obj_bbox_id
            center = self.center_points[obj_id]
            new_center_points[obj_id] = center

        self.center_points = new_center_points
        return objects_bboxes_ids
