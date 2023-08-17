import pika
import uuid

from src.config import config
from src.enum import REPORTS_INPUT_QUEUE

test_mesg = """
{
    "correlation_id": "13242421424214",
    "phones": [1,2,3,4,5,6,7,8,9,10]
}
"""

connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=config.RMQ_HOST,
            credentials=pika.PlainCredentials(
                username=config.RMQ_USER, 
                password=config.RMQ_PASSWORD
            )
        ))
channel = connection.channel()

result = channel.queue_declare(queue='', exclusive=True)
callback_queue = result.method.queue

corr_id = str(uuid.uuid4())
channel.basic_publish(
    exchange='',
    routing_key=REPORTS_INPUT_QUEUE,
    properties=pika.BasicProperties(
        reply_to=callback_queue,
        correlation_id=corr_id,
    ),
    body=test_mesg,
)

def on_response(ch, method, props, body):
    if corr_id == props.correlation_id:
        print("Received response:", body)

channel.basic_consume(
    queue=callback_queue,
    on_message_callback=on_response,
    auto_ack=True,
)

channel.start_consuming()