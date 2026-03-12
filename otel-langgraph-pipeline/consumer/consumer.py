import json
import logging
import os

from kafka import KafkaConsumer

from batch_client import submit_batch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("consumer")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "raw-logs")
GROUP = "batch-submitters"

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

    for message in consumer:

        batch = message.value

        logger.info("Submitting %d logs to Batch API", len(batch))

        batch_id = submit_batch(batch)

        logger.info("OpenAI batch created %s", batch_id)


if __name__ == "__main__":
    run()