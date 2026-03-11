"""
OTel Log Receiver  →  Batcher  →  Kafka Producer
==================================================
- Exposes POST /v1/logs  (OTLP/HTTP JSON) for the OTel Collector exporter.
- Buffers incoming log records in memory.
- Flushes to Kafka in batches of BATCH_SIZE (default 10).
- A background thread also flushes partial batches every FLUSH_INTERVAL seconds
  so logs are never stuck when volume is low.
- GET /healthz  for Docker / load-balancer health checks.
"""

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
from kafka.errors import KafkaError

# ── logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("producer")

# ── config from environment ────────────────────────────────────────────────────
KAFKA_BROKER    = os.environ.get("KAFKA_BROKER",     "kafka:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC",      "raw-logs")
BATCH_SIZE      = int(os.environ.get("BATCH_SIZE",   "10"))
FLUSH_INTERVAL  = int(os.environ.get("FLUSH_INTERVAL", "10"))   # seconds
LISTEN_PORT     = int(os.environ.get("LISTEN_PORT",  "4318"))

# ── Kafka producer ─────────────────────────────────────────────────────────────
_kafka_producer = None

def get_kafka_producer() -> KafkaProducer:
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=100,
            compression_type="gzip",
        )
    return _kafka_producer

# ── in-memory log buffer ───────────────────────────────────────────────────────
buffer: deque[str] = deque()   # each element is a raw log string
lock   = Lock()
app    = Flask(__name__)


# ── batching logic ─────────────────────────────────────────────────────────────

def _send_batch(batch: list[str]) -> None:
    try:
        p = get_kafka_producer()
        p.send(KAFKA_TOPIC, value=batch)
        p.flush()
        logger.info("[PRODUCER] Sent batch of %d log(s) to %s", len(batch), KAFKA_TOPIC)
    except KafkaError as exc:
        logger.error("[PRODUCER] Kafka send error: %s", exc)


def flush_buffer(force: bool = False) -> None:
    """
    Drain the buffer in BATCH_SIZE chunks.
    If force=True, also flush a partial (< BATCH_SIZE) batch.
    """
    with lock:
        while len(buffer) >= BATCH_SIZE or (force and buffer):
            size  = min(BATCH_SIZE, len(buffer))
            batch = [buffer.popleft() for _ in range(size)]
            _send_batch(batch)


def _periodic_flush() -> None:
    """Background thread: flush partial batches on a timer."""
    while True:
        time.sleep(FLUSH_INTERVAL)
        flush_buffer(force=True)


# ── OTel OTLP/HTTP endpoint ────────────────────────────────────────────────────

def _extract_log_records(payload: dict) -> list[str]:
    """
    Walk the OTLP JSON envelope and collect individual log record strings.
    Each record is serialised to a compact JSON string for the LLM.
    """
    records: list[str] = []
    for resource_log in payload.get("resourceLogs", []):
        # Attach resource attributes to every record for richer LLM context
        resource_attrs = {
            attr["key"]: attr.get("value", {})
            for attr in resource_log.get("resource", {}).get("attributes", [])
        }
        for scope_log in resource_log.get("scopeLogs", []):
            for record in scope_log.get("logRecords", []):
                enriched = {**record, "_resource": resource_attrs}
                records.append(json.dumps(enriched))
    return records


@app.route("/v1/logs", methods=["POST"])
def receive_logs():
    payload = request.get_json(force=True, silent=True)
    if not payload:
        return jsonify({"error": "invalid JSON body"}), 400

    records = _extract_log_records(payload)
    if not records:
        return jsonify({"status": "ok", "accepted": 0}), 200

    with lock:
        buffer.extend(records)

    flush_buffer()   # flush full batches immediately; partial waits for timer

    logger.info("[PRODUCER] Buffered %d record(s). Buffer size: %d",
                len(records), len(buffer))
    return jsonify({"status": "ok", "accepted": len(records)}), 200


@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "healthy", "buffer_size": len(buffer)}), 200


# ── graceful shutdown ──────────────────────────────────────────────────────────

def _shutdown(sig, frame):
    logger.info("[PRODUCER] Shutting down...")
    flush_buffer(force=True)
    if _kafka_producer:
        _kafka_producer.close()
    sys.exit(0)


signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)


# ── entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    Thread(target=_periodic_flush, daemon=True).start()
    logger.info("[PRODUCER] Listening on port %d  batch_size=%d  flush_interval=%ds",
                LISTEN_PORT, BATCH_SIZE, FLUSH_INTERVAL)
    app.run(host="0.0.0.0", port=LISTEN_PORT)