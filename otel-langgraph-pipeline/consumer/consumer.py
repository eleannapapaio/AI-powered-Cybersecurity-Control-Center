import json
import logging
import os
import threading
from kafka import KafkaConsumer
from batch_client import submit_batch
from batch_results_workers import run as poll_results

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("consumer")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC        = os.environ.get("KAFKA_TOPIC", "raw-logs")
GROUP        = os.environ.get("KAFKA_GROUP_ID", "langgraph-consumer-group")

logger.info("[INIT] Connecting to Kafka  broker=%s  topic=%s  group=%s", KAFKA_BROKER, TOPIC, GROUP)
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode()),
)
logger.info("[INIT] Kafka consumer ready")


def run():
    pending_batch_ids = set()
    pending_lock      = threading.Lock()

    def _poll_wrapper():
        logger.info("[POLLER] Background poller thread started")
        poll_results(pending_batch_ids)

    threading.Thread(target=_poll_wrapper, daemon=True).start()
    logger.info("[CONSUMER] Waiting for messages …")

    for message in consumer:
        batch = message.value
        logger.info(
            "[CONSUMER] Message received  partition=%s  offset=%s  logs=%d",
            message.partition, message.offset, len(batch),
        )
        try:
            batch_id = submit_batch(batch)
            with pending_lock:
                pending_batch_ids.add(batch_id)
            logger.info(
                "[CONSUMER] Batch submitted  batch_id=%s  pending=%d",
                batch_id, len(pending_batch_ids),
            )
        except Exception as exc:
            logger.error("[CONSUMER] Failed to submit batch — %s", exc, exc_info=True)


if name == "main":
    run()