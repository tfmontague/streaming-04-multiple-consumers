"""
    Name: Topaz Montague

    Project: P4 Producer with Multiple Consumers

    File Description:
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

"""

# Import necesssary modules

import pika
import sys
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # Decode the binary message body to a string
    logging.info(f" [x] Received {body.decode()}")
    # Simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # When done with task, tell the user
    logging.info(" [x] Done.")
    # Acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Define a main function to run the program
def main(hn: str = "localhost", qn: str = "task_queue"):
    """ Continuously listen for task messages on a named queue."""

    # When a statement can go wrong, use a try-except block
    try:
        # Create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # Except, if there's an error, do this
    except Exception as e:
        logging.error("ERROR: connection to RabbitMQ server failed.")
        logging.error(f"Verify the server is running on host={hn}.")
        logging.error(f"The error says: {e}")
        sys.exit(1)

    try:
        # Use the connection to create a communication channel
        channel = connection.channel()

        # Use the channel to declare a durable queue
        channel.queue_declare(queue=qn, durable=True)

        # Set the prefetch count to one to limit the number of messages 
        # being consumed and processed concurrently.
        channel.basic_qos(prefetch_count=1) 

        # Configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(queue=qn, on_message_callback=callback)

        # Log a message to the console for the user
        logging.info(" [*] Ready for work. To exit press CTRL+C")

        # Start consuming messages via the communication channel
        channel.start_consuming()

    # Except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logging.error("ERROR: something went wrong.")
        logging.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logging.info("Closing connection. Goodbye.")
        connection.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # Call the main function with the information needed
    main("localhost", "task_queue2")
