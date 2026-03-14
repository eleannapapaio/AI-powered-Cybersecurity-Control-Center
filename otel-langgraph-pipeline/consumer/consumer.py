"""
================================================================================
KAFKA-TO-LANGGRAPH CONSUMER
================================================================================

This module acts as the ingestion entry point for log processing. It consumes 
batches of logs from Kafka, directs them through the LangGraph pipeline, 
handles DLQ routing for invalid entries, and manages manual offsets.

FLOW:
  Kafka Topic (raw-logs) → Consumer → LangGraph Pipeline → DLQ (if invalid)
                                     └─→ Manual Offset Commit
================================================================================
"""

import json
import logging
import os
import threading
import signal
import sys

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from langgraph_pipeline import PipelineState, build_graph

# --- Logging Configuration ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("consumer")

# --- Environment Variables ----------------------------------------------------
KAFKA_BROKER       = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC        = os.environ.get("KAFKA_TOPIC", "raw-logs")
KAFKA_DLQ          = os.environ.get("KAFKA_DLQ", "raw-logs-dlq")
GROUP_ID           = os.environ.get("KAFKA_GROUP_ID", "langgraph-consumer-group")

# --- Kafka Client Setup -------------------------------------------------------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
    auto_offset_reset="earliest",
    enable_auto_commit=False,      # Manual commit after pipeline success
    max_poll_records=1,            # One batch per poll for precision
)

dlq_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
)

# --- LangGraph Pipeline Initialization ----------------------------------------
pipeline = build_graph()


def publish_to_dlq(invalid_records: list[dict]) -> None:
    """Send unrecoverable log records to the Dead-Letter Queue (DLQ)."""
    for record in invalid_records:
        try:
            dlq_producer.send(KAFKA_DLQ, value=record)
        except KafkaError as exc:
            logger.error("[DLQ] Failed to publish record: %s", exc)
    dlq_producer.flush()
    if invalid_records:
        logger.info("[DLQ] Published %d record(s) to %s", len(invalid_records), KAFKA_DLQ)


def process_message(message) -> None:
    """Run the LangGraph pipeline on one Kafka message batch."""
    batch: list = message.value   # List of raw log entries

    if batch is None or not isinstance(batch, list) or not batch:
        logger.warning("[CONSUMER] Empty or malformed message — skipping.")
        return

    # Normalize OTel dicts to JSON strings if necessary
    raw_log_strings: list[str] = [
        log if isinstance(log, str) else json.dumps(log)
        for log in batch
    ]

    logger.info(
        "[CONSUMER] Received %d log(s)  partition=%d  offset=%d",
        len(raw_log_strings), message.partition, message.offset,
    )

    # Initialize Graph State with Kafka provenance for traceability
    initial_state: PipelineState = {
        "all_batches":       [raw_log_strings],
        "batch_index":       0,
        "current_batch":     [],
        "pending_raw_logs":  [],
        "messages":          [],
        "llm_response_raw":  None,
        "retry_count":       0,
        "validated_entries": [],
        "invalid_buffer":    [],
        "kafka_topic":       message.topic,
        "kafka_partition":   message.partition,
        "kafka_offset":      message.offset,
        "raw_log_ids":       [],  # Populated by pipeline start_node
    }

    final_state = pipeline.invoke(initial_state)

    validated = final_state["validated_entries"]
    invalid   = final_state["invalid_buffer"]

    logger.info(
        "[CONSUMER] Done — validated=%d  buffered=%d",
        len(validated), len(invalid),
    )

    # Route errors to DLQ
    if invalid:
        publish_to_dlq(invalid)


def run() -> None:
    """Main consumer loop with signal handling for graceful shutdown."""
    logger.info("[CONSUMER] Starting — broker=%s  topic=%s  group=%s",
                KAFKA_BROKER, KAFKA_TOPIC, GROUP_ID)

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
            # Only commit offset if the pipeline completes without error
            consumer.commit()
        except Exception as exc:
            logger.error(
                "[CONSUMER] Pipeline error on partition=%d offset=%d: %s",
                message.partition, message.offset, exc,
                exc_info=True,
            )
            # No commit here: allows for reprocessing on next restart


if __name__ == "__main__":
    run()