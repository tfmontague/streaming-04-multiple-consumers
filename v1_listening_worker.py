"""
Name: Topaz Montague

Project: P4 Producer with Multiple Consumers

File Description:
Listens for task messages on the queue.
This process runs continuously. 

Make as many listening workers as you need 
(start this process in multiple terminals).

Approach:
---------
Work Queues - one task producer / many workers sharing work.

Terminal Reminders:
------------------

- Use Control c to close a terminal and end a process.
- Use the up arrow to get the last command executed.
"""

import pika
import sys
import os
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def listen_for_tasks():
    """ Continuously listen for task messages on a named queue.
    
    This function establishes a connection to a RabbitMQ server, sets up a communication channel,
    and starts consuming messages from the 'task_queue'. It uses a callback function to process 
    each received message.
    """
    
    # Create a blocking connection to the RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))

    # Use the connection to create a communication channel
    ch = connection.channel()

    # Define a callback function to be called when a message is received
    def callback(ch, method, properties, body):
        """ Define behavior on getting a message. 
        
        This callback function processes the received message by decoding it,
        simulating work by sleeping for a duration proportional to the message content,
        and acknowledging the message after processing.
        
        Args:
            ch (BlockingChannel): The channel object.
            method (Method): The method frame with delivery tag.
            properties (BasicProperties): The properties of the message.
            body (bytes): The body of the message.
        """
        
        # Decode the binary message body to a string
        logging.info(f" [x] Received {body.decode()}")

        # Simulate work by sleeping for the number of dots in the message
        time.sleep(body.count(b"."))

        # When done with task, tell the user
        logging.info(" [x] Done")

        # Acknowledge the message was received and processed 
        # (now it can be deleted from the queue)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Declare a durable queue named 'task_queue'
    # A durable queue will survive a RabbitMQ server restart
    # and help ensure messages are processed in order
    # Messages will not be deleted until the consumer acknowledges   
    ch.queue_declare(queue="task_queue", durable=True)
    logging.info(" [*] Ready for work. To exit press CTRL+C")

    # The QoS level controls the # of messages 
    # that can be in-flight (unacknowledged by the consumer) 
    # at any given time. 
    # Set the prefetch count to one to limit the number of messages 
    # being consumed and processed concurrently.
    # This helps prevent a worker from becoming overwhelmed 
    # and improve the overall system performance.
    # prefetch_count = Per consumer limit of unacknowledged messages      
    ch.basic_qos(prefetch_count=1) 
    
    # Configure the channel to listen on a specific queue,  
    # use the callback function named callback,
    # and do not auto-acknowledge the message (let the callback handle it)
    ch.basic_consume(queue="task_queue", on_message_callback=callback)

    # Start consuming messages via the communication channel
    ch.start_consuming()

if __name__ == "__main__":
    try:
        # Start listening for tasks
        listen_for_tasks()

    except KeyboardInterrupt:
        # Handle keyboard interruption gracefully
        logging.info("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)