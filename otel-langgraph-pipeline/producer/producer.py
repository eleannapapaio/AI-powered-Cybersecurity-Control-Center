import json
import logging
import os
import signal
import sys
import time
from collections import deque
from threading import Lock, Thread

from flask import Flask, jsonify, request
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw-logs")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL", "10"))
PORT = int(os.environ.get("LISTEN_PORT", "4318"))   # fixed: was "PORT"

buffer = deque()
lock = Lock()

app = Flask(__name__)


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode(),
    compression_type="gzip",
)


def send_batch(batch):
    producer.send(KAFKA_TOPIC, value=batch)
    producer.flush()
    logger.info("Sent batch %d logs", len(batch))


def flush(force=False):

    with lock:
        while len(buffer) >= BATCH_SIZE or (force and buffer):

            size = min(BATCH_SIZE, len(buffer))
            batch = [buffer.popleft() for _ in range(size)]

            send_batch(batch)


def periodic_flush():

    while True:
        time.sleep(FLUSH_INTERVAL)
        flush(force=True)


@app.route("/v1/logs", methods=["POST"])
def logs():

    payload = request.get_json(force=True)

    # Fluent Bit wrap.lua sends a single record:
    #   {"log": "<json string>", "tag": "device.cisco_asa", "timestamp": <int>}
    # The Fluent Bit HTTP output can also batch into {"logs": [...]}.
    # Handle both shapes so nothing is silently dropped.
    if "logs" in payload:
        incoming = payload["logs"]
    elif "log" in payload:
        incoming = [payload["log"]]
    else:
        # Fallback: treat whole payload as one entry
        incoming = [payload]

    with lock:
        buffer.extend(incoming)

    flush()

    return jsonify({"accepted": len(incoming)})


@app.route("/healthz")
def health():
    return jsonify({"status": "ok"})


def shutdown(sig, frame):

    logger.info("Shutting down")
    flush(force=True)
    producer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

if __name__ == "__main__":

    Thread(target=periodic_flush, daemon=True).start()

    app.run(host="0.0.0.0", port=PORT)