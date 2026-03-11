"""
Kafka Consumer  →  LangGraph Pipeline
======================================
- Reads batches of raw OTel log strings from `raw-logs` topic.
- Feeds each batch into the LangGraph pipeline (one invoke per message).
- Publishes unrecoverable (invalid) logs to `raw-logs-dlq` topic.
- Commits offsets only after successful pipeline completion.
"""

import json
import logging
import os
import signal
import sys

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from langgraph_pipeline import PipelineState, build_graph

# ── logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("consumer")

# ── config from environment ────────────────────────────────────────────────────
KAFKA_BROKER   = os.environ.get("KAFKA_BROKER",   "kafka:9092")
KAFKA_TOPIC    = os.environ.get("KAFKA_TOPIC",    "raw-logs")
KAFKA_DLQ      = os.environ.get("KAFKA_DLQ",      "raw-logs-dlq")
GROUP_ID       = os.environ.get("KAFKA_GROUP_ID", "langgraph-consumer-group")

# ── Kafka clients ──────────────────────────────────────────────────────────────
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False,      # manual commit after successful processing
    max_poll_records=1,            # one batch message at a time
)

dlq_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
)

# ── LangGraph pipeline (compiled once, reused for every message) ───────────────
pipeline = build_graph()


def publish_to_dlq(invalid_records: list[dict]) -> None:
    """Send unrecoverable log records to the dead-letter queue topic."""
    for record in invalid_records:
        try:
            dlq_producer.send(KAFKA_DLQ, value=record)
        except KafkaError as exc:
            logger.error("[DLQ] Failed to publish record: %s", exc)
    dlq_producer.flush()
    if invalid_records:
        logger.info("[DLQ] Published %d record(s) to %s", len(invalid_records), KAFKA_DLQ)


def process_message(message) -> None:
    """Run the LangGraph pipeline on one Kafka message (= one batch of logs)."""
    batch: list = message.value   # list of raw log strings sent by producer

    if not isinstance(batch, list) or not batch:
        logger.warning("[CONSUMER] Empty or malformed message — skipping.")
        return

    # Convert OTel dicts → JSON strings if the producer sent dicts
    raw_log_strings: list[str] = [
        log if isinstance(log, str) else json.dumps(log)
        for log in batch
    ]

    logger.info(
        "[CONSUMER] Received %d log(s)  partition=%d  offset=%d",
        len(raw_log_strings), message.partition, message.offset,
    )

    initial_state: PipelineState = {
        "all_batches":       [raw_log_strings],   # single batch per Kafka message
        "batch_index":       0,
        "current_batch":     [],
        "pending_raw_logs":  [],
        "messages":          [],
        "llm_response_raw":  None,
        "retry_count":       0,
        "validated_entries": [],
        "invalid_buffer":    [],
        # Kafka provenance — stored in each invalid record for traceability
        "kafka_topic":       message.topic,
        "kafka_partition":   message.partition,
        "kafka_offset":      message.offset,
    }

    final_state = pipeline.invoke(initial_state)

    validated = final_state["validated_entries"]
    invalid   = final_state["invalid_buffer"]

    logger.info(
        "[CONSUMER] Done — validated=%d  buffered=%d",
        len(validated), len(invalid),
    )

    # Publish invalid records to DLQ
    if invalid:
        publish_to_dlq(invalid)


def run() -> None:
    logger.info("[CONSUMER] Starting — broker=%s  topic=%s  group=%s",
                KAFKA_BROKER, KAFKA_TOPIC, GROUP_ID)

    # Graceful shutdown on SIGTERM / SIGINT
    def _shutdown(sig, frame):
        logger.info("[CONSUMER] Shutting down…")
        consumer.close()
        dlq_producer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    for message in consumer:
        try:
            process_message(message)
            # Commit offset only after the pipeline completes successfully
            consumer.commit()
        except Exception as exc:
            logger.error(
                "[CONSUMER] Pipeline error on partition=%d offset=%d: %s",
                message.partition, message.offset, exc,
                exc_info=True,
            )
            # Do NOT commit — message will be reprocessed after restart


if __name__ == "__main__":
    run()