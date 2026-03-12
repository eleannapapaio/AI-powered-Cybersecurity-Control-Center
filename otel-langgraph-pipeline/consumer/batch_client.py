import json
import tempfile

from openai import OpenAI

client = OpenAI()

MODEL = "gpt-4.1"


SYSTEM_PROMPT = """
Parse this OpenTelemetry log into structured JSON.

Return ONLY JSON matching this schema:

{
"timestamp":"",
"level":"",
"message":"",
"service":{"name":"","version":"","env":""},
"trace":{"trace_id":"","span_id":""},
"event":{"category":"","action":"","duration_ms":0},
"user":{"id":"","ip":""},
"error":{"code":"","stack_trace":""},
"metadata":{"region":"","container_id":""}
}
"""


def submit_batch(logs):

    requests = []

    for i, log in enumerate(logs):

        requests.append({
            "custom_id": f"log-{i}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": MODEL,
                "temperature": 0,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": log}
                ]
            }
        })

    tmp = tempfile.NamedTemporaryFile(delete=False, mode="w")

    for r in requests:
        tmp.write(json.dumps(r) + "\n")

    tmp.close()

    batch_file = client.files.create(
        file=open(tmp.name, "rb"),
        purpose="batch"
    )

    batch = client.batches.create(
        input_file_id=batch_file.id,
        endpoint="/v1/chat/completions",
        completion_window="24h"
    )

    return batch.id