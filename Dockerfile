FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    && pip install paho-mqtt pyserial \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY vedirect_to_mqtt.py .

CMD ["python", "vedirect_to_mqtt.py"]
