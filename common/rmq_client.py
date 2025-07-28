import pika
import json
import time
from pydantic import BaseModel, ValidationError
from typing import Callable, Type, Dict, Any, List, Optional, Union
import logging # Импортируем logging

from common.config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, MAIN_EXCHANGE_NAME, setup_logging
from common.exceptions import RabbitMQConnectionError
from common.models import (
    ClientCreatedMessage, InvoiceRequestMessage, ContractStatusUpdateMessage,
    ClientUpdatedMessage, TaskCreatedMessage, AcknowledgementMessage
)

MESSAGE_MODELS: Dict[str, Type[BaseModel]] = {
    "client_created": ClientCreatedMessage,
    "request_invoice": InvoiceRequestMessage,
    "contract_status_update": ContractStatusUpdateMessage,
    "client_updated": ClientUpdatedMessage,
    "task_created": TaskCreatedMessage,
    "acknowledgement": AcknowledgementMessage,
}

class RabbitMQClient:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.logger = setup_logging(f"{service_name}.RabbitMQClient") # Получаем логгер
        self._connection = None
        self._channel = None
        self._connect()

    def _connect(self):
        """Connects to RabbitMQ server."""
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            self._channel.exchange_declare(exchange=MAIN_EXCHANGE_NAME, exchange_type='direct', durable=True)
            self.logger.info(f"Connected to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        except pika.exceptions.AMQPConnectionError as e:
            self.logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise RabbitMQConnectionError(f"Failed to connect to RabbitMQ: {e}")
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred during RabbitMQ connection.") # We use exception to output stack trace
            raise RabbitMQConnectionError(f"An unexpected error occurred during RabbitMQ connection: {e}")

    def _ensure_connection(self):
        """Checks and reconnects if the connection is lost."""
        if not self._connection or self._connection.is_closed:
            self.logger.warning("Reconnecting to RabbitMQ...")
            self._connect()
        elif not self._channel or self._channel.is_closed:
            self.logger.warning("Recreating RabbitMQ channel...")
            self._channel = self._connection.channel()
            self._channel.exchange_declare(exchange=MAIN_EXCHANGE_NAME, exchange_type='direct', durable=True)


    def publish_message(self, message: BaseModel, routing_key: str):
        """Publish message to RabbitMQ"""
        self._ensure_connection()
        try:
            message_body = message.model_dump_json()
            self._channel.basic_publish(
                exchange=MAIN_EXCHANGE_NAME,
                routing_key=routing_key,
                body=message_body.encode('utf-8'),
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                )
            )
            self.logger.info(f"Sent '{message.action}' to '{routing_key}': {message.model_dump_json()}")
        except Exception as e:
            self.logger.exception(f"Error publishing message.")
            raise

    def start_consuming(self, queue_name: str, callback: Callable[[Any, Any, Any, bytes], None], routing_key: str):
        """Begins consuming messages from the specified queue."""
        self._ensure_connection()
        try:
            # The queue and binding are declared in definitions.json; here, we just make sure that they exist.
            self._channel.queue_declare(queue=queue_name, durable=True)
            self._channel.queue_bind(exchange=MAIN_EXCHANGE_NAME, queue=queue_name, routing_key=routing_key)

            self._channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
            self.logger.info(f"Waiting for messages in queue '{queue_name}' with routing key '{routing_key}'. To exit press CTRL+C")
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            self.logger.error(f"AMQP Connection Error during consuming: {e}. Attempting to reconnect...")
            self._close()
            time.sleep(5)
            self._connect()
            self.start_consuming(queue_name, callback, routing_key)
        except Exception as e:
            self.logger.exception(f"Error consuming messages.")
            raise

    def _close(self):
        """Closes connection to RabbitMQ."""
        if self._connection and self._connection.is_open:
            self._connection.close()
            self.logger.info("Closed RabbitMQ connection.")

    def __del__(self):
        self._close()