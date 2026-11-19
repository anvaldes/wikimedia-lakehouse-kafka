FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    GOOGLE_CLOUD_PROJECT=lab-experiments-514316

WORKDIR /app

COPY ingestion/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY ingestion/producer/ .

CMD ["python", "main.py"]