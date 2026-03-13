import json
import logging
import tempfile
from openai import OpenAI

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

client = OpenAI()
MODEL  = "gpt-4.1"

SYSTEM_PROMPT = """
Parse this Fluent Bit log entry into structured JSON.

The input may be:
- Fluent Bit plain logs (e.g. "[2026/03/13 12:10:21] [info] message")
- Fluent Bit forwarded structured logs (JSON format)

Extract fields when available and map them to the schema below.
If a field does not exist in the log, return an empty string "" or 0 for numbers.

Return ONLY valid JSON matching this schema:

{
  "timestamp":"", "level":"", "message":"",
  "service":{"name":"","version":"","env":""},
  "trace":{"trace_id":"","span_id":""},
  "event":{"category":"","action":"","duration_ms":0},
  "user":{"id":"","ip":""},
  "error":{"code":"","stack_trace":""},
  "metadata":{"region":"","container_id":""}
}
"""


def submit_batch(logs):
    logger.info("[BATCH_CLIENT] Building %d request(s)", len(logs))
    requests = [
        {
            "custom_id": f"log-{i}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": MODEL,
                "temperature": 0,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": log},
                ],
            },
        }
        for i, log in enumerate(logs)
    ]

    tmp = tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".jsonl")
    for r in requests:
        tmp.write(json.dumps(r) + "\n")
    tmp.close()
    logger.info("[BATCH_CLIENT] JSONL written  path=%s  requests=%d", tmp.name, len(requests))

    batch_file = client.files.create(file=open(tmp.name, "rb"), purpose="batch")
    logger.info("[BATCH_CLIENT] File uploaded to OpenAI  file_id=%s", batch_file.id)

    batch = client.batches.create(
        input_file_id=batch_file.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )
    logger.info(
        "[BATCH_CLIENT] Batch job created  batch_id=%s  status=%s  model=%s",
        batch.id, batch.status, MODEL,
    )
    return batch.id