import os
import logging

# RabbitMQ Connection Settings
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')

# Exchange Name
MAIN_EXCHANGE_NAME = 'main_exchange'

# PostgreSQL Database Settings (будут читаться из ENV для каждого сервиса)
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper() # Logging level(?) (DEBUG, INFO, WARNING, ERROR, CRITICAL)

def setup_logging(service_name: str):
    """Configures the basic logger for the service."""
    logger = logging.getLogger(service_name)
    logger.setLevel(LOG_LEVEL)

    # If there are already handlers, do not add new ones (prevents log duplication)
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Console output
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger