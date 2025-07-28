import os
import threading
import json
import time
import uuid
from typing import List

from pydantic import ValidationError

from common.rmq_client import RabbitMQClient, MESSAGE_MODELS
from common.db_client import \
    DBClient  # We use DBClient to demonstrate that DLQ Processor can also write to the database.
from common.config import setup_logging


class DLQProcessor:
    def __init__(self, rmq_client: RabbitMQClient, db_client: DBClient):
        self.rmq_client = rmq_client
        self.db_client = db_client
        self.logger = setup_logging("DLQProcessor")
        self.dlq_queues = {
            "1c": "1c_dlq",
            "dms": "dms_dlq",
            "crm": "crm_dlq"
        }
        self._initialize_db_schema()  # Initialize the schema for storing DLQ messages

    def _initialize_db_schema(self):
        self.db_client.create_table_if_not_exists(
            "dead_messages",
            """
            message_id VARCHAR(255) PRIMARY KEY,
            original_queue VARCHAR(50) NOT NULL,
            dead_letter_reason VARCHAR(255),
            source_system VARCHAR(50),
            action_type VARCHAR(50),
            message_body JSONB,
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """
        )
        self.logger.info("[DLQProcessor] Database schema for dead_messages initialized.")

    def on_dlq_message_callback(self, ch, method, properties, body: bytes):
        try:
            message_data = json.loads(body.decode('utf-8'))
            action_type = message_data.get('action')
            source_system = message_data.get('source_system', 'unknown')

            self.logger.warning(
                f"Received DEAD LETTER from {method.routing_key} (from {source_system}): {message_data}")

            # Retrieve the reason for being placed in DLQ (if available)
            headers = properties.headers
            dlq_reason = None
            if headers and 'x-death' in headers and isinstance(headers['x-death'], list):
                # x-death headers could be list, so we take the last element
                first_x_death_entry = headers['x-death'][0]
                dlq_reason = first_x_death_entry.get('reason')
                self.logger.warning(f"DLQ Reason: {dlq_reason}")

            # Analysis imitation and possible reprocessing
            self.logger.info(f"Analyzing dead message (action: {action_type}, source: {source_system})...")
            time.sleep(2)  # Work imitation
            self.logger.info(f"Attempting to reprocess or log permanently...")

            #Saving dead messages in bd
            message_id = message_data.get('client_id') or message_data.get('invoice_id') or message_data.get(
                'contract_id') or message_data.get('task_id') or f"DLQ_MSG_{uuid.uuid4().hex}"

            self.db_client.execute_query(
                """
                INSERT INTO dead_messages (message_id, original_queue, dead_letter_reason, source_system, action_type,
                                           message_body)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (message_id) DO
                UPDATE SET
                    original_queue = EXCLUDED.original_queue,
                    dead_letter_reason = EXCLUDED.dead_letter_reason,
                    source_system = EXCLUDED.source_system,
                    action_type = EXCLUDED.action_type,
                    message_body = EXCLUDED.message_body,
                    received_at = CURRENT_TIMESTAMP;
                """,
                (message_id, method.routing_key, dlq_reason, source_system, action_type, json.dumps(message_data))
            )
            self.logger.info(f"[DLQProcessor] Dead message {message_id} saved to DB.")

            # After processing or saving, confirm the message.
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.logger.info(f"Acknowledged dead message from {method.routing_key}.")

        except json.JSONDecodeError:
            self.logger.error(f"DLQ JSON Decode Error: {body.decode('utf-8')}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag,
                          requeue=False)  # Unable to decode, sending to DLQ (or other special queue)
        except ValidationError as e:
            self.logger.error(f"DLQ Pydantic Validation Error: {e}. Message: {body.decode('utf-8')}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred in DLQ processor for message: {body.decode('utf-8')}.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_processing(self):
        self.logger.info("Starting DLQ processing service...")
        threads: List[threading.Thread] = []
        for service_name, queue_name in self.dlq_queues.items():
            # DLQs are listened to by routing_key, not by a specific queue,
            # but here I simply specify the DLQ queue as the target for listening.
            # routing_key for dlx_exchange was dead_messages.<service_name>
            routing_key = f"dead_messages.{service_name}"

            thread = threading.Thread(
                target=self.rmq_client.start_consuming,
                args=(queue_name, self.on_dlq_message_callback, routing_key),
                daemon=True
            )
            threads.append(thread)
            thread.start()
            self.logger.info(f"Started consuming from DLQ: {queue_name} with routing key {routing_key}")

        # Keeping the main flow alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("DLQ Processor stopped.")


if __name__ == "__main__":
    service_name = os.getenv('SERVICE_NAME', 'DLQ_Processor_Default')
    main_logger = setup_logging(service_name)
    main_logger.info(f"Starting {service_name} service...")

    rmq_client = RabbitMQClient(service_name=service_name)

    # DLQ Processor also needs a database to store dead messages.
    db_client = DBClient(
        service_name=service_name,
        db_host=os.getenv('DB_HOST', 'db_dlq_processor'),  # Separate БД for DLQ
        db_name=os.getenv('DB_NAME', 'dlq_db'),
        db_user=os.getenv('DB_USER', 'userdlq'),
        db_password=os.getenv('DB_PASSWORD', 'passworddlq')
    )

    dlq_processor = DLQProcessor(rmq_client, db_client)
    dlq_processor.start_processing()