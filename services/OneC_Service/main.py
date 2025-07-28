import os
import threading
import time
from common.rmq_client import RabbitMQClient
from common.db_client import DBClient
from services.OneC_Service.producer import OneCProducer
from services.OneC_Service.consumer import OneCConsumer
from common.config import setup_logging


def run_1c_service():
    service_name = os.getenv('SERVICE_NAME', '1C_Default')
    main_logger = setup_logging(service_name)
    main_logger.info(f"Starting {service_name} service...")

    consumer_rmq_client = RabbitMQClient(service_name=f"{service_name}.Consumer")
    producer_rmq_client = RabbitMQClient(service_name=f"{service_name}.Producer")

    # Initializing the DB client with environment variables
    db_client = DBClient(
        service_name=service_name,
        db_host=os.getenv('DB_HOST'),
        db_name=os.getenv('DB_NAME'),
        db_user=os.getenv('DB_USER'),  # Use environment variable
        db_password=os.getenv('DB_PASSWORD') # Use environment variable
    )

    producer = OneCProducer(producer_rmq_client, db_client)
    consumer = OneCConsumer(consumer_rmq_client, db_client)

    consumer_thread = threading.Thread(target=consumer.start_consuming, daemon=True)
    consumer_thread.start()
    main_logger.info(f"[{service_name}] Consumer thread started.")

    try:
        producer.start_producing()
    except KeyboardInterrupt:
        main_logger.info(f"[{service_name}] Producer stopped by user.")
    except Exception as e:
        main_logger.exception(f"[{service_name}] Producer encountered an error.")
    finally:
        producer_rmq_client._close()
        consumer_rmq_client._close()


if __name__ == "__main__":
    run_1c_service()