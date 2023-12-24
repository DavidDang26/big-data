from processing import get_json_result
import pika
import json
import os
from dotenv import load_dotenv

load_dotenv()


def callback(ch, method, properties, body):
    message = json.loads(body)
    get_json_result(message["fileName"])


# Connection parameters
connection_params = pika.ConnectionParameters(os.getenv('RABBIT_MQ_HOST'))
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue='logic_trigger')

# Set up the consumer and specify the callback function
channel.basic_consume(queue='logic_trigger',
                      on_message_callback=callback,
                      auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

# get_json_result("24-12-2023.csv")
