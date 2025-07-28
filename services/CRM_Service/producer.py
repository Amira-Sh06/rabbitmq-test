import time
import uuid
import random

from common.rmq_client import RabbitMQClient
from common.models import ClientUpdatedMessage, TaskCreatedMessage
from common.db_client import DBClient

class CrmProducer:
    def __init__(self, rmq_client: RabbitMQClient, db_client: DBClient):
        self.rmq_client = rmq_client
        self.db_client = db_client

    def start_producing(self):
        print("[CRM Producer] Starting message production...")
        client_counter = 3000
        while True:
            # Scenario 1: SRM updates customer data (send в 1С)
            client_id = f"CLIENT_{uuid.uuid4().hex[:8].upper()}" # Imitation of an existing customer
            updated_fields = {
                "address": f"Test str no.{random.randint(1,100)}",
                "city": random.choice(["Moscow", "Almaty", "New York"])
            }
            client_update_message = ClientUpdatedMessage(
                client_id=client_id,
                updated_fields=updated_fields,
                source_system="CRM"
            )
            self.rmq_client.publish_message(client_update_message, routing_key="to_1c")

            # Scenario 2: CRM creates a new task for 1C (e.g., data request)
            task_id = f"TASK_{uuid.uuid4().hex[:8].upper()}"
            task_message = TaskCreatedMessage(
                task_id=task_id,
                client_id=client_id, # We link to the same client
                description=f"Check customers debt {client_id}",
                assigned_to="1C Finance Department",
                source_system="CRM"
            )
            self.rmq_client.publish_message(task_message, routing_key="to_1c")


            client_counter += 1
            time.sleep(random.uniform(4, 8))