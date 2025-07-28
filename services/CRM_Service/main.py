import os
import threading

from common.rmq_client import RabbitMQClient
from common.db_client import DBClient
from services.CRM_Service.producer import CrmProducer
from services.CRM_Service.consumer import CrmConsumer
from common.config import setup_logging

def run_crm_service():
    service_name = os.getenv('SERVICE_NAME', 'CRM_Default')
    logger = setup_logging(service_name)  # Инициализируем логгер для главного файла
    logger.info(f"[{service_name}] Starting CRM service...")  # Используем логгер

    # Initialization of RabbitMQ client
    rmq_client = RabbitMQClient(service_name=service_name)

    # Initialization of DB client
    db_client = DBClient(
        service_name=service_name,
        db_host=os.getenv('DB_HOST'),
        db_name=os.getenv('DB_NAME'),
        db_user=os.getenv('DB_USER'),
        db_password=os.getenv('DB_PASSWORD')
    )

    consumer_rmq_client = RabbitMQClient(service_name=f"{service_name}.Consumer")
    producer_rmq_client = RabbitMQClient(service_name=f"{service_name}.Producer")

    producer = CrmProducer(producer_rmq_client, db_client) # Transferring to db_client
    consumer = CrmConsumer(consumer_rmq_client, db_client) # Transferring to db_client

    # We launch the consumer in a separate thread so that it constantly listens for messages, simulating the work of a message broker.
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
        # Убедимся, что соединения RabbitMQ корректно закрываются при завершении работы
        # (Хотя __del__ должен сработать, явное закрытие лучше)
        producer_rmq_client._close()
        consumer_rmq_client._close()  # Важно закрыть и соединение потребителя

    if __name__ == "__main__":
        run_crm_service()