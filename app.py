import os
from azure.servicebus import ServiceBusClient
from flask import Flask, jsonify
import threading
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load connection string and queue name from environment variables
CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")
QUEUE_NAME = os.getenv("SERVICE_BUS_QUEUE_NAME")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Shared list to store received messages
messages = []

app = Flask(__name__)


def receive_messages():
    servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)
    with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME)
        with receiver:
            while True:
                received_msgs = receiver.receive_messages(max_message_count=10, max_wait_time=5)
                for msg in received_msgs:
                    messages.append(str(msg))
                    receiver.complete_message(msg)
                    logger.info(f"Received and completed message: {msg}")


def start_background_task():
    thread = threading.Thread(target=receive_messages)
    thread.daemon = True  # Daemon threads are abruptly stopped at shutdown
    thread.start()


@app.before_request
def before_first_request():
    start_background_task()


@app.route("/")
def root():
    # Return the received messages
    return jsonify({"messages": messages})


if __name__ == "__main__":
    app.run()
