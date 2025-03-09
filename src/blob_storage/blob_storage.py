# src/blob_storage/blob_service.py
import os
import logging
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient

class BlobStorageService:
    """Service for interacting with Azure Blob Storage"""
    
    def __init__(self, connection_string):
        """Initialize the BlobStorageService with the Azure connection string
        
        Args:
            connection_string (str): Azure Storage connection string
        """
        self.connection_string = connection_string
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.async_blob_service_client = None  # Lazy initialization for async client
    
    def create_container_if_not_exists(self, container_name):
        """Create a container if it doesn't already exist
        
        Args:
            container_name (str): Name of the container to create
            
        Returns:
            ContainerClient: Azure container client
        """
        container_client = self.blob_service_client.get_container_client(container_name)
        
        if not container_client.exists():
            logging.info(f"Creating container: {container_name}")
            container_client.create_container()
        
        return container_client
    
    async def upload_video(self, container_name, blob_name, content, content_type="video/mp4"):
        """Upload a video file to Azure Blob Storage (async version)
        
        Args:
            container_name (str): Name of the container
            blob_name (str): Name to give the blob
            content (bytes or file-like object): Content to upload
            content_type (str): MIME type of the content
            
        Returns:
            str: URL of the uploaded blob
        """
        # Create container if it doesn't exist (using sync client)
        self.create_container_if_not_exists(container_name)
        
        # Initialize async client if needed
        if not self.async_blob_service_client:
            self.async_blob_service_client = AsyncBlobServiceClient.from_connection_string(self.connection_string)
        
        # Get async container and blob clients
        async_container_client = self.async_blob_service_client.get_container_client(container_name)
        async_blob_client = async_container_client.get_blob_client(blob_name)
        
        # Set the content type for the blob
        content_settings = ContentSettings(content_type=content_type)
        
        logging.info(f"Uploading blob: {blob_name} to container: {container_name}")
        await async_blob_client.upload_blob(content, content_settings=content_settings, overwrite=True)
        
        # Construct the URL (we can't easily get it from the async client)
        account_name = self.blob_service_client.account_name
        url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}"
        
        return url
    
    # Add a sync version for non-async contexts
    def upload_video_sync(self, container_name, blob_name, content, content_type="video/mp4"):
        """Upload a video file to Azure Blob Storage (sync version)
        
        Args:
            container_name (str): Name of the container
            blob_name (str): Name to give the blob
            content (bytes or file-like object): Content to upload
            content_type (str): MIME type of the content
            
        Returns:
            str: URL of the uploaded blob
        """
        container_client = self.create_container_if_not_exists(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        # Set the content type for the blob
        content_settings = ContentSettings(content_type=content_type)
        
        logging.info(f"Uploading blob: {blob_name} to container: {container_name}")
        blob_client.upload_blob(content, content_settings=content_settings, overwrite=True)
        
        return blob_client.url
    
    async def download_video(self, container_name, blob_name):
        """Download a video file from Azure Blob Storage (async version)
        
        Args:
            container_name (str): Name of the container
            blob_name (str): Name of the blob
            
        Returns:
            bytes: Content of the blob
        """
        # Initialize async client if needed
        if not self.async_blob_service_client:
            self.async_blob_service_client = AsyncBlobServiceClient.from_connection_string(self.connection_string)
        
        # Get async container and blob clients
        async_container_client = self.async_blob_service_client.get_container_client(container_name)
        async_blob_client = async_container_client.get_blob_client(blob_name)
        
        logging.info(f"Downloading blob: {blob_name} from container: {container_name}")
        download = await async_blob_client.download_blob()
        
        return await download.readall()