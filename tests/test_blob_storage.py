# test_storage.py
import asyncio
import logging
import os
from dotenv import load_dotenv
from src.storage.blob_storage import BlobStorageService
from src.common.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()

async def test_blob_storage():
    """Test the blob storage service"""
    # Initialize the blob storage service
    blob_storage = BlobStorageService(Config.AZURE_STORAGE_CONNECTION_STRING)
    
    # Create test container
    test_container = "test-container"
    blob_storage.create_container_if_not_exists(test_container)
    
    # Create a test file
    test_content = b"This is a test file content"
    test_blob_name = "test-blob.txt"
    
    # Upload the test file
    url = await blob_storage.upload_video(
        test_container,
        test_blob_name,
        test_content,
        content_type="text/plain"
    )
    logging.info(f"Uploaded test blob to: {url}")
    
    # List blobs
    blobs = blob_storage.list_blobs(test_container)
    logging.info(f"Blobs in container: {blobs}")
    
    # Download the test file
    downloaded_content = await blob_storage.download_video(test_container, test_blob_name)
    logging.info(f"Downloaded content: {downloaded_content}")
    
    # Verify the content matches
    assert downloaded_content == test_content
    logging.info("Content verification successful")
    
    # Clean up
    await blob_storage.delete_blob(test_container, test_blob_name)
    logging.info("Test completed successfully")

if __name__ == "__main__":
    # Validate configuration
    Config.validate()
    
    # Run the test
    asyncio.run(test_blob_storage())