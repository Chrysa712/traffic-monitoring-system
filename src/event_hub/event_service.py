import json
import logging
from typing import Dict, List, Any

from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

class EventHubService:
    """Service for interacting with Azure Event Hub"""
    
    def __init__(self, connection_string: str, eventhub_name: str):
        """Initialize the EventHubService
        
        Args:
            connection_string (str): Event Hub connection string
            eventhub_name (str): Event Hub name
        """
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
    
    async def send_event(self, event_data: Dict[str, Any], properties: Dict[str, Any] = None):
        """Send a single event to Event Hub
        
        Args:
            event_data (Dict[str, Any]): Event data to send
            properties (Dict[str, Any], optional): Event properties for filtering
            
        Returns:
            None
        """
        producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string,
            eventhub_name=self.eventhub_name
        )
        
        async with producer:
            # Create a batch
            event_data_batch = await producer.create_batch()
            
            # Create event
            event = EventData(json.dumps(event_data).encode('utf-8'))
            
            # Add properties if provided
            if properties:
                event.properties = properties
            
            # Add event to batch
            if not event_data_batch.try_add(event):
                raise ValueError("Event too large for batch")
            
            # Send batch
            await producer.send_batch(event_data_batch)
    
    async def send_events(self, events: List[Dict[str, Any]], property_key: str = None):
        """Send multiple events to Event Hub
        
        Args:
            events (List[Dict[str, Any]]): List of event data to send
            property_key (str, optional): Key to use for properties from event data
            
        Returns:
            None
        """
        if not events:
            return
        
        producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string,
            eventhub_name=self.eventhub_name
        )
        
        async with producer:
            # Create a batch
            event_data_batch = await producer.create_batch()
            
            for event_data in events:
                # Create event
                event = EventData(json.dumps(event_data).encode('utf-8'))
                
                # Add properties if key provided
                if property_key and property_key in event_data:
                    event.properties = {property_key: event_data[property_key]}
                
                # Add event to batch, if it's full then send and create a new one
                if not event_data_batch.try_add(event):
                    await producer.send_batch(event_data_batch)
                    event_data_batch = await producer.create_batch()
                    if not event_data_batch.try_add(event):
                        raise ValueError("Event too large for batch")
            
            # Send any remaining events
            if len(event_data_batch) > 0:
                await producer.send_batch(event_data_batch)
    
    async def process_events(self, callback, checkpoint_store_connection_string: str, consumer_group: str = "$Default"):
        """Process events from Event Hub with checkpointing
        
        Args:
            callback (callable): Callback function for processing events
            checkpoint_store_connection_string (str): Storage connection string for checkpointing
            consumer_group (str, optional): Event Hub consumer group
            
        Returns:
            None
        """
        # Create a checkpoint store
        checkpoint_store = BlobCheckpointStore.from_connection_string(
            checkpoint_store_connection_string,
            container_name="eventhub-checkpoints"
        )
        
        # Create a consumer client
        consumer = EventHubConsumerClient.from_connection_string(
            conn_str=self.connection_string,
            consumer_group=consumer_group,
            eventhub_name=self.eventhub_name,
            checkpoint_store=checkpoint_store
        )
        
        async def on_event(partition_context, event):
            # Extract data from event
            event_data = json.loads(event.body_as_str())
            
            # Process the event using the callback
            await callback(event_data, event.properties)
            
            # Update checkpoint
            await partition_context.update_checkpoint(event)
        
        # Start receiving events
        async with consumer:
            await consumer.receive(on_event)