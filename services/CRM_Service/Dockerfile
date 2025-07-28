FROM python:3.10-slim-buster

WORKDIR /app

COPY ./common /app/common
COPY ./services/CRM_Service/requirements.txt /app/services/CRM_Service/requirements.txt
RUN pip install --no-cache-dir -r /app/services/CRM_Service/requirements.txt

COPY ./services/CRM_Service/main.py /app/services/CRM_Service/main.py
COPY ./services/CRM_Service/consumer.py /app/services/CRM_Service/consumer.py
COPY ./services/CRM_Service/producer.py /app/services/CRM_Service/producer.py

CMD ["python", "/app/services/CRM_Service/main.py"]