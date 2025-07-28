FROM python:3.10-slim-buster

WORKDIR /app

COPY ./common /app/common
COPY ./services/DMS_Service/requirements.txt /app/services/DMS_Service/requirements.txt
RUN pip install --no-cache-dir -r /app/services/DMS_Service/requirements.txt

COPY ./services/DMS_Service/main.py /app/services/DMS_Service/main.py
COPY ./services/DMS_Service/consumer.py /app/services/DMS_Service/consumer.py
COPY ./services/DMS_Service/producer.py /app/services/DMS_Service/producer.py

CMD ["python", "/app/services/DMS_Service/main.py"]