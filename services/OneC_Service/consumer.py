import json
import random

from common.rmq_client import RabbitMQClient, MESSAGE_MODELS
from common.db_client import DBClient
from common.models import (
    ContractStatusUpdateMessage, ClientUpdatedMessage, InvoiceRequestMessage,
    TaskCreatedMessage, AcknowledgementMessage
)
from pydantic import ValidationError
from common.config import setup_logging # Import setup_logging

class OneCConsumer:
    def __init__(self, rmq_client: RabbitMQClient, db_client: DBClient):
        self.rmq_client = rmq_client
        self.db_client = db_client
        self.queue_name = "1c_inbox"
        self.routing_key = "to_1c"
        self.logger = setup_logging("1C.Consumer") # Get logger
        self._initialize_db_schema()

    def _initialize_db_schema(self):
        self.db_client.create_table_if_not_exists(
            "contracts",
            """
            contract_id VARCHAR(255) PRIMARY KEY,
            client_id VARCHAR(255) NOT NULL,
            new_status VARCHAR(50) NOT NULL,
            service_type VARCHAR(100),
            updated_by_system VARCHAR(50),
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """
        )
        self.db_client.create_table_if_not_exists(
            "clients",
            """
            client_id VARCHAR(255) PRIMARY KEY,
            last_updated_by_system VARCHAR(50),
            last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            additional_data JSONB
            """
        )
        self.db_client.create_table_if_not_exists(
            "tasks",
            """
            task_id VARCHAR(255) PRIMARY KEY,
            client_id VARCHAR(255) NOT NULL,
            description TEXT NOT NULL,
            assigned_to VARCHAR(100),
            source_system VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """
        )
        self.logger.info("Database schemas initialized.")


    def on_message_callback(self, ch, method, properties, body: bytes):
        try:
            message_data = json.loads(body.decode('utf-8'))
            action_type = message_data.get('action')

            # Simulation of a random processing error
            if random.random() < 0.1:
                self.logger.error(f"Simulating a processing error for message: {message_data.get('action')}:{message_data.get('client_id')}")
                # Reject the message by sending it to DLQ
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            if action_type not in MESSAGE_MODELS:
                self.logger.warning(f"Unknown message action type: {action_type}. Message: {message_data}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) # Send в DLQ
                return

            model_class = MESSAGE_MODELS[action_type]
            message_obj = model_class.model_validate(message_data)

            self.logger.info(f"Received message from {message_obj.source_system} with action '{action_type}': {message_obj.model_dump_json()}")

            if isinstance(message_obj, ContractStatusUpdateMessage):
                self.logger.info(f"Processing Contract Status Update for contract {message_obj.contract_id}: new status {message_obj.new_status}")
                self.db_client.execute_query(
                    """
                    INSERT INTO contracts (contract_id, client_id, new_status, service_type, updated_by_system)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (contract_id) DO UPDATE SET
                        new_status = EXCLUDED.new_status,
                        service_type = EXCLUDED.service_type,
                        updated_by_system = EXCLUDED.updated_by_system,
                        received_at = CURRENT_TIMESTAMP;
                    """,
                    (message_obj.contract_id, message_obj.client_id, message_obj.new_status,
                     message_obj.service_type, message_obj.source_system)
                )
                self.logger.info(f"Contract {message_obj.contract_id} saved/updated in DB.")

                ack_message = AcknowledgementMessage(
                    original_message_id=message_obj.contract_id,
                    status="success",
                    source_system="1C",
                    destination_system=message_obj.source_system
                )
                self.rmq_client.publish_message(ack_message, routing_key=f"to_{message_obj.source_system.lower()}")

            elif isinstance(message_obj, ClientUpdatedMessage):
                self.logger.info(f"Processing Client Update for client {message_obj.client_id}: updated fields {message_obj.updated_fields}")
                self.db_client.execute_query(
                    """
                    INSERT INTO clients (client_id, last_updated_by_system, additional_data)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (client_id) DO UPDATE SET
                        last_updated_by_system = EXCLUDED.last_updated_by_system,
                        additional_data = EXCLUDED.additional_data,
                        last_updated_at = CURRENT_TIMESTAMP;
                    """,
                    (message_obj.client_id, message_obj.source_system, json.dumps(message_obj.updated_fields))
                )
                self.logger.info(f"Client {message_obj.client_id} update saved in DB.")

                ack_message = AcknowledgementMessage(
                    original_message_id=message_obj.client_id,
                    status="success",
                    source_system="1C",
                    destination_system=message_obj.source_system
                )
                self.rmq_client.publish_message(ack_message, routing_key=f"to_{message_obj.source_system.lower()}")

            elif isinstance(message_obj, TaskCreatedMessage):
                self.logger.info(f"Processing new Task for client {message_obj.client_id}, description: {message_obj.description}")
                self.db_client.execute_query(
                    """
                    INSERT INTO tasks (task_id, client_id, description, assigned_to, source_system)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (task_id) DO NOTHING;
                    """,
                    (message_obj.task_id, message_obj.client_id, message_obj.description,
                     message_obj.assigned_to, message_obj.source_system)
                )
                self.logger.info(f"Task {message_obj.task_id} saved in DB.")

                ack_message = AcknowledgementMessage(
                    original_message_id=message_obj.task_id,
                    status="success",
                    source_system="1C",
                    destination_system=message_obj.source_system
                )
                self.rmq_client.publish_message(ack_message, routing_key=f"to_{message_obj.source_system.lower()}")

            elif isinstance(message_obj, AcknowledgementMessage):
                self.logger.info(f"Received Acknowledgement for message ID {message_obj.original_message_id} with status {message_obj.status} from {message_obj.source_system}")
            else:
                self.logger.warning(f"Unhandled message type for 1C: {action_type}. Message: {message_data}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            self.logger.error(f"JSON Decode Error: {body.decode('utf-8')}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) # Отправляем в DLQ
        except ValidationError as e:
            self.logger.error(f"Pydantic Validation Error: {e}. Message: {body.decode('utf-8')}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) # Отправляем в DLQ
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred during processing for message: {body.decode('utf-8')}.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) # Отправляем в DLQ

    def start_consuming(self):
        self.logger.info(f"Starting consumption from queue '{self.queue_name}'...")
        self.rmq_client.start_consuming(self.queue_name, self.on_message_callback, self.routing_key)