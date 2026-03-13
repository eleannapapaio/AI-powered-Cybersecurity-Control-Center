import json
import logging
import os
import threading

from kafka import KafkaConsumer

from batch_client import submit_batch
from batch_results_workers import run as poll_results

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumer")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "raw-logs")
GROUP = os.environ.get("KAFKA_GROUP_ID", "langgraph-consumer-group")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode()),
)


def run():

    logger.info("Consumer started")

    # Shared set of pending batch IDs — producer thread adds, poller removes
    pending_batch_ids = set()
    pending_lock = threading.Lock()

    # Start the background poller that fetches completed OpenAI batch results
    def _poll_wrapper():
        # batch_results_workers.run() expects a mutable list/set it can remove from
        poll_results(pending_batch_ids)

    poller = threading.Thread(target=_poll_wrapper, daemon=True)
    poller.start()
    logger.info("Batch results poller started")

    for message in consumer:

        batch = message.value

        logger.info("Submitting %d logs to Batch API", len(batch))

        try:
            batch_id = submit_batch(batch)
            with pending_lock:
                pending_batch_ids.add(batch_id)
            logger.info("OpenAI batch created %s  (pending=%d)", batch_id, len(pending_batch_ids))
        except Exception as exc:
            logger.error("Failed to submit batch: %s", exc)


if __name__ == "__main__":
    run()