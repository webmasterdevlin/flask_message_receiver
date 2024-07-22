import asyncio
from flask import Flask, jsonify
import threading
import logging
from dotenv import load_dotenv
from message_bus import MessageBus

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Shared list to store received messages
messages = []

app = Flask(__name__)

message_bus = MessageBus()


async def process_message(msg):
    message_content = str(msg)
    messages.append(message_content)
    logger.info(f"Received and completed message: {message_content}")


def receive_messages():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(message_bus.start_consuming(process_message))


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
