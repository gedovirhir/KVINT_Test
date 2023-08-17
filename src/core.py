import pika

from typing import Callable

from src.config import config

def get_pika_connection(on_connection_open: Callable):
    return pika.SelectConnection(
        parameters=pika.ConnectionParameters(
            host=config.RMQ_HOST,
            credentials=pika.PlainCredentials(
                username=config.RMQ_USER, 
                password=config.RMQ_PASSWORD
            )
        ),
        on_open_callback=on_connection_open
    )
    