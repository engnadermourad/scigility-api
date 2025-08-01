from celery import Celery
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Read RabbitMQ connection details from environment variables
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'user')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'password')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')  # Adjust to match your Docker container name
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')

# Format the Celery broker URL using the environment variables
RABBITMQ_CELERY_BROKER_URL = f'pyamqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//'

# Initialize Celery with RabbitMQ as the broker
app = Celery('tasks', broker=RABBITMQ_CELERY_BROKER_URL)

# Example Celery task
@app.task
def add(x, y):
    return x + y
