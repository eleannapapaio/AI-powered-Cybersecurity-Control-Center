FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .
COPY langgraph_pipeline.py .
#COPY batch_client.py .
#COPY batch_results_workers.py .

CMD ["python", "consumer.py"]