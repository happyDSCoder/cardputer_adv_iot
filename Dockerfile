FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY daemon.py .
COPY config.json .

ENV CONFIG_PATH=/app/config.json

CMD ["python", "-u", "daemon.py"]
