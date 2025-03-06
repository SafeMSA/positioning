import pika
import json
import time
import random

# RabbitMQ connection parameters
RABBITMQ_HOST = 'rabbitmq1'
QUEUE_NAME = 'position_updates'

def connect_to_rabbitmq():
    #Attempts to connect to RabbitMQ, retrying until successful.
    credentials = pika.PlainCredentials('myuser', 'mypassword')
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.exchange_declare(exchange='dlx.exchange', exchange_type='direct')
            channel.queue_declare(queue='dlx.queue')
            channel.queue_bind(exchange='dlx.exchange', queue='dlx.queue', routing_key='dlx')

            channel.queue_declare(
                queue=QUEUE_NAME,
                arguments={
                    'x-dead-letter-exchange': 'dlx.exchange',   # Send unprocessed messages here
                    'x-dead-letter-routing-key': 'dlx',          # Routing key
                    'x-message-ttl': 5,                          # Time to live
                    'x-max-length': 100                         # Queue max length
                }
            )
            print("Connected to RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ not available, retrying in 5 seconds...")
            time.sleep(5)
        

def publish_position(channel):
    position = {
        'id': int(random.uniform(0, 100)),
        'x': round(random.uniform(-1000, 1000), 2),
        'y': round(random.uniform(-1000, 1000), 2),
        'z': round(random.uniform(-1000, 1000), 2),
        'timestamp': time.time()
    }
    
    message = json.dumps(position)
    channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=message)
    print(f"Sent: {message}")

# Attempt to connect to RabbitMQ
connection, channel = connect_to_rabbitmq()

while True:
    publish_position(channel)
    time.sleep(random.uniform(0.5, 3))  # Random delay between 0.5 to 3 seconds
