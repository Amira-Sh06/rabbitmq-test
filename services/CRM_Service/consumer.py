import json
import random

from common.rmq_client import RabbitMQClient, MESSAGE_MODELS
from common.db_client import DBClient
from common.models import ClientCreatedMessage, InvoiceRequestMessage, AcknowledgementMessage
from pydantic import ValidationError
from common.config import setup_logging

class CrmConsumer:
    def __init__(self, rmq_client: RabbitMQClient, db_client: DBClient):
        self.rmq_client = rmq_client
        self.db_client = db_client
        self.queue_name = "crm_inbox"
        self.routing_key = "to_crm"
        self.logger = setup_logging("CRM.Consumer")
        self._initialize_db_schema()

    def _initialize_db_schema(self):
        self.db_client.create_table_if_not_exists(
            "clients",
            """
            client_id VARCHAR(255) PRIMARY KEY,
            client_name VARCHAR(255) NOT NULL,
            email VARCHAR(255),
            phone VARCHAR(50),
            source_system VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """
        )
        self.db_client.create_table_if_not_exists(
            "invoices",
            """
            invoice_id VARCHAR(255) PRIMARY KEY,
            client_id VARCHAR(255) NOT NULL,
            amount DECIMAL(10, 2) NOT NULL,
            currency VARCHAR(10) NOT NULL,
            description TEXT,
            source_system VARCHAR(50),
            requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """
        )
        self.logger.info("Database schemas initialized.")

    def on_message_callback(self, ch, method, properties, body: bytes):
        try:
            message_data = json.loads(body.decode('utf-8'))
            action_type = message_data.get('action')

            # Имитация случайной ошибки обработки (10% шанс)
            if random.random() < 0.1:
                self.logger.error(f"Simulating a processing error for message: {message_data.get('action')}:{message_data.get('client_id')}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            if action_type not in MESSAGE_MODELS:
                self.logger.warning(f"Unknown message action type: {action_type}. Message: {message_data}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            model_class = MESSAGE_MODELS[action_type]
            message_obj = model_class.model_validate(message_data)

            self.logger.info(f"Received message from {message_obj.source_system} with action '{action_type}': {message_obj.model_dump_json()}")

            if isinstance(message_obj, ClientCreatedMessage):
                self.logger.info(f"Creating new client from 1C: {message_obj.client_id} - {message_obj.client_name}")
                self.db_client.execute_query(
                    """
                    INSERT INTO clients (client_id, client_name, email, phone, source_system)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (client_id) DO NOTHING;
                    """,
                    (message_obj.client_id, message_obj.client_name, message_obj.email,
                     message_obj.phone, message_obj.source_system)
                )
                self.logger.info(f"Client {message_obj.client_id} created in DB.")

                ack_message = AcknowledgementMessage(
                    original_message_id=message_obj.client_id,
                    status="success",
                    source_system="CRM",
                    destination_system=message_obj.source_system
                )
                self.rmq_client.publish_message(ack_message, routing_key=f"to_{message_obj.source_system.lower()}")

            elif isinstance(message_obj, InvoiceRequestMessage):
                self.logger.info(f"Processing Invoice Request {message_obj.invoice_id} for client {message_obj.client_id}, amount {message_obj.amount} {message_obj.currency}")
                self.db_client.execute_query(
                    """
                    INSERT INTO invoices (invoice_id, client_id, amount, currency, description, source_system)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (invoice_id) DO NOTHING;
                    """,
                    (message_obj.invoice_id, message_obj.client_id, message_obj.amount,
                     message_obj.currency, message_obj.description, message_obj.source_system)
                )
                self.logger.info(f"Invoice {message_obj.invoice_id} request saved in DB.")

                ack_message = AcknowledgementMessage(
                    original_message_id=message_obj.invoice_id,
                    status="success",
                    source_system="CRM",
                    destination_system=message_obj.source_system
                )
                self.rmq_client.publish_message(ack_message, routing_key=f"to_{message_obj.source_system.lower()}")

            elif isinstance(message_obj, AcknowledgementMessage):
                self.logger.info(f"Received Acknowledgement for message ID {message_obj.original_message_id} with status {message_obj.status} from {message_obj.source_system}")

            else:
                self.logger.warning(f"Unhandled message type for CRM: {action_type}. Message: {message_data}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError:
            self.logger.error(f"JSON Decode Error: {body.decode('utf-8')}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except ValidationError as e:
            self.logger.error(f"Pydantic Validation Error: {e}. Message: {body.decode('utf-8')}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred: {e}. Message: {body.decode('utf-8')}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        self.logger.info(f"Starting consumption from queue '{self.queue_name}'...")
        self.rmq_client.start_consuming(self.queue_name, self.on_message_callback, self.routing_key)