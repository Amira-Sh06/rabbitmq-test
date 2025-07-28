import time
import uuid
import random

from common.rmq_client import RabbitMQClient
from common.db_client import DBClient
from common.models import ClientCreatedMessage, InvoiceRequestMessage, AcknowledgementMessage
from common.config import MAIN_EXCHANGE_NAME, setup_logging # Import setup_logging

class OneCProducer:
    def __init__(self, rmq_client: RabbitMQClient, db_client: DBClient):
        self.rmq_client = rmq_client
        self.db_client = db_client
        self.logger = setup_logging("1C.Producer") # Get logger

    def start_producing(self):
        self.logger.info("Starting message production...")
        client_counter = 1000
        while True:
            client_id = f"CLIENT_{uuid.uuid4().hex[:8].upper()}"
            client_message = ClientCreatedMessage(
                client_id=client_id,
                client_name=f"Тестовый Клиент {client_counter}",
                email=f"client_{client_counter}@example.com",
                phone=f"+7900{random.randint(1000000, 9999999)}",
                source_system="1C"
            )
            self.rmq_client.publish_message(client_message, routing_key="to_crm")
            self.rmq_client.publish_message(client_message, routing_key="to_dms")
            self.logger.info(f"Published ClientCreatedMessage for client {client_id} to CRM and DMS.")

            invoice_id = f"INV_{uuid.uuid4().hex[:8].upper()}"
            invoice_request = InvoiceRequestMessage(
                invoice_id=invoice_id,
                client_id=client_id,
                amount=round(random.uniform(1000, 50000), 2),
                currency=random.choice(["RUB", "KZT", "USD"]),
                description=f"Счет за услуги для клиента {client_id}",
                source_system="1C"
            )
            self.rmq_client.publish_message(invoice_request, routing_key="to_crm")
            self.logger.info(f"Published InvoiceRequestMessage for invoice {invoice_id} to CRM.")

            client_counter += 1
            time.sleep(random.uniform(2, 5))