import pika
import json
import time
import random

# RabbitMQ connection parameters
RABBITMQ_HOST = 'rabbitmq1'
QUEUE_NAME = 'position_updates'

def publish_position(channel):
    position = {
        'id': random.uniform(0, 100),
        'x': round(random.uniform(-1000, 1000), 2),
        'y': round(random.uniform(-1000, 1000), 2),
        'z': round(random.uniform(-1000, 1000), 2),
        'timestamp': time.time()
    }
    
    message = json.dumps(position)
    channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=message)
    print(f"Sent: {message}")

# Set up RabbitMQ connection
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME)


while True:
    publish_position(channel)
    time.sleep(random.uniform(0.5, 3))  # Random delay between 0.5 to 3 seconds
