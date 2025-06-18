FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    curl \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Установка зависимостей
COPY requirements.txt /app/requirements.txt

WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app/node_monitor.py /app/node_monitor.py

CMD ["python", "-u", "node_monitor.py"]
