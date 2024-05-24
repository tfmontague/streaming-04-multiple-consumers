"""
    Name: Topaz Montague

    Project: P4 Producer with Multiple Consumers

    File Description:
    This program reads tasks from tasks.csv and sends messages to a queue on the RabbitMQ server.
"""
# Import necessary modules

import pika
import sys
import csv
import webbrowser
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def open_rabbitmq_admin_site():
    """Open the RabbitMQ Admin website automatically."""
    webbrowser.open_new("http://localhost:15672/#/queues")
    logging.info("Opened RabbitMQ Admin site")

def get_messages_from_csv(file_path: str):
    """
    Reads messages from a CSV file.

    Parameters:
        file_path (str): The path to the CSV file containing messages.

    Returns:
        list: A list of messages read from the CSV file.
    """
    messages = []
    try:
        with open(file_path, mode='r') as file:
            reader = csv.reader(file)
            for row in reader:
                if row:
                    messages.append(row[0])
        logging.info(f"Successfully read messages from {file_path}")
    except FileNotFoundError:
        logging.error(f"Error: The file {file_path} was not found.")
        sys.exit(1)
    return messages

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): The host name or IP address of the RabbitMQ server.
        queue_name (str): The name of the queue.
        message (str): The message to be sent to the queue.
    """
    try:
        # Create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # Use the connection to create a communication channel
        ch = conn.channel()
        # Use the channel to declare a durable queue
        ch.queue_declare(queue=queue_name, durable=True)
        # Use the channel to publish a message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # Log a message to the console for the user
        logging.info(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # Close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
if __name__ == "__main__":
    open_rabbitmq_admin_site()
    file_path = 'tasks.csv'
    messages = get_messages_from_csv(file_path)
    for message in messages:
        send_message("localhost", "task_queue2", message)
