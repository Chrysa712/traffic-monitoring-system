# scripts/test_blob_trigger.py
import os
import sys
import logging
import uuid
import asyncio

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.blob_storage.blob_storage import BlobStorageService
from src.common.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)

async def test_blob_trigger(video_path):
    """Test the blob trigger by uploading a video
    
    Args:
        video_path (str): Path to a local video file
    """
    # Validate configuration
    Config.validate()
    
    # Initialize blob storage service
    blob_service = BlobStorageService(Config.AZURE_STORAGE_CONNECTION_STRING)
    
    # Create a unique blob name
    video_id = f"test-video-{uuid.uuid4()}"
    blob_name = f"{video_id}.mp4"
    
    # Create container if it doesn't exist
    blob_service.create_container_if_not_exists(Config.RAW_VIDEOS_CONTAINER)
    
    # Upload the video
    logging.info(f"Uploading video {blob_name} to container {Config.RAW_VIDEOS_CONTAINER}")
    with open(video_path, 'rb') as f:
        content = f.read()
        url = await blob_service.upload_video(Config.RAW_VIDEOS_CONTAINER, blob_name, content)
    
    logging.info(f"Video uploaded to {url}")
    logging.info(f"The Azure Function should be triggered to process this video.")
    logging.info(f"Check the Azure Function logs to verify processing.")
    logging.info(f"After processing, check {Config.PROCESSED_SEGMENTS_CONTAINER} for video segments.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_blob_trigger.py <path_to_video_file>")
        sys.exit(1)
    
    video_path = sys.argv[1]
    if not os.path.exists(video_path):
        print(f"Error: Video file not found: {video_path}")
        sys.exit(1)
    
    asyncio.run(test_blob_trigger(video_path))