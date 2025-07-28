# Enterprise Integration Demo with RabbitMQ, PostgreSQL, and Docker

This project demonstrates a robust enterprise integration pattern using **RabbitMQ** as a message broker and **PostgreSQL** as persistent storage for various simulated business systems. It showcases asynchronous message processing, data validation with Pydantic, and essential production-ready features like Dead Letter Queues (DLQ) and structured logging.

The core idea is to simulate communication between different enterprise systems (e.g., 1C, DMS, CRM) without direct API calls, relying on a message queue for loose coupling and resilience.

---

## Requirements

* **Python 3.10+**
* **Docker** and **Docker Compose** installed on your system.
    * [Install Docker Engine](https://docs.docker.com/engine/install/)
    * [Install Docker Compose](https://docs.docker.com/compose/install/)

---

## Project Structure

```
.
├── common/                  # Shared utilities, models, RabbitMQ/DB clients
│   ├── __init__.py
│   ├── config.py            # Global configurations, logging setup
│   ├── db_client.py         # PostgreSQL database client
│   ├── exceptions.py        # Custom exceptions
│   ├── models.py            # Pydantic data models for messages
│   └── rmq_client.py        # RabbitMQ client
├── rabbitMQ/                # RabbitMQ specific configurations
│   ├── __init__.py
│   └── definitions.json     # RabbitMQ exchanges, queues, bindings, and DLQ setup
├── services/                # Individual microservices (simulated systems)
│   ├── CRM_Service/
│   │   ├── __init__.py
│   │   ├── Dockerfile
│   │   ├── main.py
│   │   ├── consumer.py
│   │   ├── producer.py
│   │   └── requirements.txt
│   ├── DMS_Service/
│   │   ├── __init__.py
│   │   ├── Dockerfile
│   │   ├── main.py
│   │   ├── consumer.py
│   │   ├── producer.py
│   │   └── requirements.txt
│   ├── OneC_Service/
│   │   ├── __init__.py
│   │   ├── Dockerfile
│   │   ├── main.py          # Entry point for 1C service
│   │   ├── consumer.py      # Consumes messages for 1C
│   │   ├── producer.py      # Produces messages from 1C
│   │   └── requirements.txt
│   └── dlq_processor/       # Dedicated service for handling Dead Letter Queue messages
│       ├── __init__.py
│       ├── Dockerfile
│       ├── main.py
│       └── requirements.txt
├── .gitignore               # Files and directories to ignore for Git
└── docker-compose.yml       # Docker Compose configuration for all services
└── README.md                # This file
```

---

---

## Features

* **Asynchronous Messaging:** Loose coupling between services using RabbitMQ.
* **Message Validation:** All messages are validated using **Pydantic** models, ensuring data integrity across systems.
* **Persistent Storage:** Each simulated service (`1C`, `DMS`, `CRM`) and the `DLQ Processor` utilize a dedicated **PostgreSQL** database, ensuring data isolation and persistence.
* **Dead Letter Queues (DLQ):** Messages that fail processing (e.g., due to errors, invalid format, or TTL expiration) are automatically routed to dedicated DLQs for later analysis or reprocessing, ensuring no data loss.
* **Structured Logging:** Uses Python's `logging` module for clear, consistent, and configurable output. Logs include relevant message context (e.g., `client_id`, `action_type`) to simplify debugging and monitoring.
* **Error Simulation:** Consumers include a simulated error rate (e.g., 10% chance of failure) to effectively demonstrate and test DLQ functionality.
* **Dockerized Environment:** All components run in isolated Docker containers, ensuring easy setup, portability, and consistent environments.

---

## Getting Started

### Running the Project

1.  **Clone the repository (if you haven't already):**
    ```bash
    git clone [https://github.com/YourGitHubUsername/EnterpriseIntegrationDemo.git](https://github.com/YourGitHubUsername/EnterpriseIntegrationDemo.git)
    cd EnterpriseIntegrationDemo # Navigate to the root of the project
    ```
2.  **Start the services:**
    Navigate to the root directory of the project (where `docker-compose.yml` is located) in your terminal and run:
    ```bash
    docker compose down -v # Cleans up any previous runs, ensuring fresh state and RabbitMQ config
    docker compose up --build -d # Builds images and runs services in detached mode
    ```

3.  **Monitor the services:**
    * To view logs from all services in real-time:
        ```bash
        docker compose logs -f
        ```
    * To view logs from a specific service (e.g., 1C):
        ```bash
        docker compose logs -f service_1c
        ```
        (Replace `service_1c` with `service_dms`, `service_crm`, `dlq_processor`, or `rabbitmq` as needed.)
    * Open your web browser and navigate to the **RabbitMQ Management UI**: [http://localhost:15672](http://localhost:15672) (default credentials: `guest`/`guest`).
        * Go to the "Queues" tab to observe message flow and accumulation in `_inbox` and `_dlq` queues. You'll see messages accumulating in `_dlq` queues when errors are simulated.
        * *(Optional: Add Screenshot of RabbitMQ UI here)*
    * (Optional) To inspect PostgreSQL data:
        * Get the ID of a running PostgreSQL container (e.g., `db_1c`): `docker ps`
        * Connect to the PostgreSQL shell inside the container:
            `docker exec -it <CONTAINER_ID_OR_NAME> psql -U <DB_USER> -d <DB_NAME>`
            (e.g., `docker exec -it db_1c psql -U user1c -d onec_db`)
        * Run SQL queries, e.g., `SELECT * FROM clients;` or `SELECT * FROM dead_messages;` (for `db_dlq_processor`).

### Stopping the Project

To stop and remove all running containers, networks, and volumes (including persistent database data), run:

bash
docker compose down -v

## How it Works

* **RabbitMQ:** Acts as the central nervous system. Producers send messages to the main_exchange (a direct exchange configured in definitions.json), which routes them to the appropriate _inbox queues based on routing_key. For example, to_dms sends messages to the dms_inbox queue.
* **Producers (1C, DMS, CRM):** Simulate sending business events (e.g., `ClientCreatedMessage` for new client creation, `ContractStatusUpdateMessage` for contract status changes, `InvoiceRequestMessage` for billing requests) into the system. These messages are serialized as JSON and published to RabbitMQ.
* **Consumers (1C, DMS, 1CRM):** Each consumer listens to its respective _inbox queue. Upon receiving a message:
    1.  They validate the incoming message payload using Pydantic models, ensuring its structure and data types are correct.
    2.  They save the processed data to their dedicated PostgreSQL database.
    3.  They send an `AcknowledgementMessage` back to the source system via RabbitMQ, confirming successful processing
    4.  **& Dead Letter Queues:**  If any error occurs during validation or processing (including simulated errors, where a message has a ~10% chance of failure), the message is negatively acknowledged (`ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False`)) and automatically moved to the corresponding `Dead Letter Queue` (`_dlq`). Messages also move to DLQ if their TTL (`x-message-ttl` configured in `definitions.json`) expires.
* **DLQ Processor:** This dedicated service continuously monitors all `_dlq` queues. When a dead message is received, it logs the error details (including the `x-death` headers providing the reason for dead-lettering) and saves the raw message alongside error metadata to its own dedicated PostgreSQL database (`dlq_db`) for audit and potential future reprocessing.
* (Optional: Add a simple JSON example of a message here, e.g., ClientCreatedMessage)
* 
```
{
  "action": "ClientCreated",
  "client_id": "CLIENT_ABCD1234",
  "client_name": "Тестовый Клиент 123",
  "email": "client_123@example.com",
  "phone": "+79001234567",
  "source_system": "1C"
}
```

---

## Future Enhancements (Ideas for further development)

* **Retries and Backoff Strategies:** Implement logic for consumers to retry message processing a limited number of times before sending to DLQ, potentially with exponential backoff.
* **Observability:** Integrate Prometheus and Grafana for comprehensive metric collection and visualization (e.g., message rates, error rates, consumer lag).
* **Health Checks:** Implement more sophisticated health checks for services (e.g., checking DB connectivity, RabbitMQ channel status).
* **Configuration Management:** Use environment variables or a dedicated config service for sensitive data (e.g., database credentials) instead of hardcoding. (Partially done with `os.getenv`).
* **Scalability:** Configure multiple instances of consumer services in Docker Compose to handle higher loads and demonstrate load balancing.
* **Error Notifications:** Integrate with notification services (e.g., Slack, Email) to alert about critical errors in DLQs.
* **Idempotency:** Implement mechanisms to ensure that processing a message multiple times (due to retries or network issues) does not lead to duplicate data.

---

## Project Background

This project was developed as part of an internship assignment focused on implementing a robust message broker solution for enterprise system communication (1C, CRM, DMS). 
It served as a hands-on exercise to gain practical experience with Python backend development, asynchronous messaging patterns, and building resilient distributed systems. 
This experience has significantly deepened my understanding of inter-system communication, data validation, and error handling in a real-world context.

---

Feel free to explore, modify, and expand this project! Contributions and feedback are welcome.

---