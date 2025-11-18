FROM python:3.9-slim AS base

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


FROM base AS producer

# Копирование исходного кода
COPY load_kafka.py .
COPY train.csv .

CMD ["python", "load_kafka.py"]


FROM base AS query

# Копирование исходного кода
COPY clickhouse_init.py .
COPY query.sql .

CMD ["python", "clickhouse_init.py"]