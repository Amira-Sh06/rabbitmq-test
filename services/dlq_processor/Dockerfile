FROM python:3.10-slim-buster

WORKDIR /app

COPY ./common /app/common
COPY ./services/dlq_processor/requirements.txt /app/services/dlq_processor/requirements.txt
RUN pip install --no-cache-dir -r /app/services/dlq_processor/requirements.txt

COPY ./services/dlq_processor/main.py /app/services/dlq_processor/main.py

CMD ["python", "/app/services/dlq_processor/main.py"]