"""
    Name: Topaz Montague

    Project: P4 Producer with Multiple Consumers

    File Description:
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.


"""
# Import necessary modules

import pika
import sys
import webbrowser
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    logging.info("")
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        logging.info("Opening RabbitMQ Admin website")
        logging.info("")

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:
        # Create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # Use the connection to create a communication channel
        ch = conn.channel()
        # Use the channel to declare a durable queue
        ch.queue_declare(queue=queue_name, durable=True)
        # Use the channel to publish a message to the queue
        ch.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
        )
        # Log a message to the console for the user
        logging.info(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # Close the connection to the server
        conn.close()

if __name__ == "__main__":  
    # Ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    # Get the message from the command line
    message = " ".join(sys.argv[1:]) or "Second task....."
    # Send the message to the queue
    send_message("localhost", "task_queue2", message)