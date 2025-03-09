import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class Config:
    """Configuration for the traffic monitoring system"""
    
    # Azure Storage
    AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    RAW_VIDEOS_CONTAINER = os.getenv('RAW_VIDEOS_CONTAINER', 'raw-footage')
    PROCESSED_SEGMENTS_CONTAINER = os.getenv('PROCESSED_SEGMENTS_CONTAINER', 'processed-videos')
    
    # Event Hub
    EVENT_HUB_CONNECTION_STRING = os.getenv('EVENT_HUB_CONNECTION_STRING')
    EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME', 'video-segments')
    
    # Cosmos DB
    COSMOS_DB_CONNECTION_STRING = os.getenv('COSMOS_DB_CONNECTION_STRING')
    COSMOS_DB_NAME = os.getenv('COSMOS_DB_NAME', 'traffic-monitoring')
    COSMOS_DB_CONTAINER = os.getenv('COSMOS_DB_CONTAINER', 'vehicle-events')
    
    # Video Processing
    SEGMENT_DURATION_SECONDS = int(os.getenv('SEGMENT_DURATION_SECONDS', '120'))  # 2 minutes
    
    @classmethod
    def validate(cls):
        """Validate that all required configuration is present"""
        required_vars = [
            ('AZURE_STORAGE_CONNECTION_STRING', 'Azure Blob Storage connection string'),
            ('EVENT_HUB_CONNECTION_STRING', 'Event Hub connection string')
        ]
        
        missing = []
        for var_name, description in required_vars:
            if not getattr(cls, var_name):
                missing.append(f"{var_name} ({description})")
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")