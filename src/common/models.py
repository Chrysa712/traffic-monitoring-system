from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from datetime import datetime
import uuid

class VideoMetadata(BaseModel):
    """Metadata for a video file"""
    video_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    blob_name: str
    container_name: str
    content_length: int
    content_type: str = "video/mp4"
    upload_time: datetime = Field(default_factory=datetime.utcnow)

class VideoSegment(BaseModel):
    """Information about a video segment"""
    segment_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    video_id: str
    segment_number: int
    blob_name: str
    container_name: str
    start_time: int  # Start time in seconds from the beginning of the original video
    duration: int  # Duration in seconds
    processed: bool = False
