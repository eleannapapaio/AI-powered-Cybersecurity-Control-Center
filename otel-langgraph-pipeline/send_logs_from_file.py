"""
Read raw logs from a file and send them to the producer in batches.

Supported formats:
  - JSON array file:   [ {...}, {...} ]
  - NDJSON file:       one JSON object per line
  - Plain text file:   one raw log string per line

Usage:
  python send_logs_from_file.py --file my_logs.json
  python send_logs_from_file.py --file my_logs.txt --format text
"""

import argparse
import json
import time
import requests

PRODUCER_URL = "http://localhost:4318/v1/logs"
CHUNK_SIZE   = 10   # match your BATCH_SIZE


def wrap_as_otlp(raw_logs: list) -> dict:
    """Wrap a list of raw log strings or dicts into a minimal OTLP envelope."""
    records = []
    for log in raw_logs:
        body = log if isinstance(log, str) else json.dumps(log)
        records.append({
            "timeUnixNano": str(int(time.time() * 1e9)),
            "severityText": "INFO",
            "body": {"stringValue": body},
        })
    return {
        "resourceLogs": [{
            "resource": {"attributes": []},
            "scopeLogs": [{"logRecords": records}]
        }]
    }


def load_logs(filepath: str, fmt: str) -> list:
    with open(filepath, "r", encoding="utf-8") as f:
        if fmt == "json":
            data = json.load(f)
            return data if isinstance(data, list) else [data]
        elif fmt == "ndjson":
            return [json.loads(line) for line in f if line.strip()]
        else:  # plain text
            return [line.strip() for line in f if line.strip()]


def send(logs: list) -> None:
    total   = len(logs)
    batches = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
    print(f"Sending {total} log(s) in {batches} batch(es) of up to {CHUNK_SIZE}…")

    for i in range(0, total, CHUNK_SIZE):
        chunk   = logs[i : i + CHUNK_SIZE]
        payload = wrap_as_otlp(chunk)
        resp    = requests.post(PRODUCER_URL, json=payload, timeout=10)
        print(f"  Batch {i // CHUNK_SIZE + 1}/{batches}  →  "
              f"status={resp.status_code}  accepted={resp.json().get('accepted', '?')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file",   required=True, help="Path to log file")
    parser.add_argument("--format", default="json",
                        choices=["json", "ndjson", "text"],
                        help="File format (default: json)")
    args = parser.parse_args()

    logs = load_logs(args.file, args.format)
    send(logs)