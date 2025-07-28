FROM python:3.10-slim-buster

WORKDIR /app

COPY ./common /app/common
COPY ./services/OneC_Service/requirements.txt /app/services/OneC_Service/requirements.txt
RUN pip install --no-cache-dir -r /app/services/OneC_Service/requirements.txt

COPY ./services/OneC_Service/main.py /app/services/OneC_Service/main.py
COPY ./services/OneC_Service/consumer.py /app/services/OneC_Service/consumer.py
COPY ./services/OneC_Service/producer.py /app/services/OneC_Service/producer.py

CMD ["python", "/app/services/OneC_Service/main.py"]