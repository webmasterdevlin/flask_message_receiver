import os
from functools import wraps
from azure.servicebus import ServiceBusMessage, ServiceBusReceiveMode
from azure.servicebus.aio import ServiceBusClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

AZURE_SERVICE_BUS = {
    "ConnectionString": os.getenv("SERVICE_BUS_CONNECTION_STR"),
    "QueueName": os.getenv("SERVICE_BUS_QUEUE_NAME"),
}


def _reconnect_if_required(func):
    @wraps(func)
    async def _func_wrapper(self, *args, **kwargs):
        try:
            return await func(self, *args, **kwargs)
        except Exception as e:
            print(f"Error: {e}. Attempting to reconnect...")
            self.servicebus_client = ServiceBusClient.from_connection_string(self.connection_str)
            return await func(self, *args, **kwargs)
    return _func_wrapper


class MessageBus:
    def __init__(self):
        self.service_callback_handler = None
        self.connection_str = AZURE_SERVICE_BUS["ConnectionString"]
        self.queue_name = AZURE_SERVICE_BUS["QueueName"]
        self.servicebus_client = ServiceBusClient.from_connection_string(self.connection_str)

    @_reconnect_if_required
    async def send(self, message, correlation_id=None):
        """Sends a message to the configured queue"""
        async with self.servicebus_client.get_queue_sender(queue_name=self.queue_name) as sender:
            service_bus_message = ServiceBusMessage(message, message_id=correlation_id)
            await sender.send_messages(service_bus_message)

    @_reconnect_if_required
    async def start_consuming(self, service_callback_handler):
        """
        Starts consuming from the configured queue and calls method callbackHandler for callback
        """
        self.service_callback_handler = service_callback_handler
        async with self.servicebus_client.get_queue_receiver(queue_name=self.queue_name,
                                                             receive_mode=ServiceBusReceiveMode.PEEK_LOCK) as receiver:
            print(f"Waiting for messages from {self.queue_name}...")
            async for msg in receiver:
                try:
                    await self.service_callback_handler(msg)
                    await receiver.complete_message(msg)
                except Exception as e:
                    print(f"Message processing failed: {e}")
                    await receiver.abandon_message(msg)
