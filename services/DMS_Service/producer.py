import time
import uuid
import random

from common.rmq_client import RabbitMQClient
from common.models import ContractStatusUpdateMessage
from common.db_client import DBClient
from common.config import MAIN_EXCHANGE_NAME, setup_logging # Import setup_logging

class DmsProducer:
    def __init__(self, rmq_client: RabbitMQClient, db_client: DBClient):
        self.rmq_client = rmq_client
        self.db_client = db_client
        self.logger = setup_logging("DMS.Producer") # Initialize logger

    def start_producing(self):
        self.logger.info("[DMS Producer] Starting message production...")
        contract_counter = 2000
        while True:
            # Scenario: DMS updates the contract status (sent to 1C)
            contract_id = f"CONTR_{uuid.uuid4().hex[:8].upper()}"
            client_id_for_contract = f"CLIENT_{uuid.uuid4().hex[:8].upper()}" # Simulation of an existing customer
            contract_status_update = ContractStatusUpdateMessage(
                contract_id=contract_id,
                client_id=client_id_for_contract,
                new_status=random.choice(["active", "expired", "suspended", "pending"]),
                service_type=random.choice(["Medical", "Dental", "Vision"]),
                source_system="DMS"
            )
            self.rmq_client.publish_message(contract_status_update, routing_key="to_1c")
            self.logger.info(f"Published ContractStatusUpdate for contract {contract_id} to 1C.")

            contract_counter += 1
            time.sleep(random.uniform(3, 7))