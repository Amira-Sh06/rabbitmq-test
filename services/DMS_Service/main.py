import os
import threading
import time

from common.rmq_client import RabbitMQClient
from common.db_client import DBClient
from services.DMS_Service.producer import DmsProducer
from services.DMS_Service.consumer import DmsConsumer
from common.config import setup_logging

def run_dms_service():
    service_name = os.getenv('SERVICE_NAME', 'DMS_Default')
    logger = setup_logging(service_name)
    logger.info(f"[{service_name}] Starting DMS service...")

    # Initializing the DB client with environment variables
    db_client = DBClient(
        service_name=service_name,
        db_host=os.getenv('DB_HOST'),
        db_name=os.getenv('DB_NAME'),
        db_user=os.getenv('DB_USER'),  # Use environment variable
        db_password=os.getenv('DB_PASSWORD') # Use environment variable
    )

    consumer_rmq_client = RabbitMQClient(service_name=f"{service_name}.Consumer")
    producer_rmq_client = RabbitMQClient(service_name=f"{service_name}.Producer")

    producer = DmsProducer(producer_rmq_client, db_client)
    consumer = DmsConsumer(consumer_rmq_client, db_client)

    consumer_thread = threading.Thread(target=consumer.start_consuming, daemon=True)
    consumer_thread.start()
    logger.info(f"[{service_name}] Consumer thread started.")

    try:
        producer.start_producing()
    except KeyboardInterrupt:
        logger.info(f"[{service_name}] Producer stopped by user.")
    except Exception as e:
        logger.exception(f"[{service_name}] Producer encountered an error.")
    finally:
        producer_rmq_client._close()
        consumer_rmq_client._close()


if __name__ == "__main__":
    run_dms_service()