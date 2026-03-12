import argparse
import json
import time
import requests
import csv
import re  # Προσθήκη για parsing του KVP format

PRODUCER_URL = "http://localhost:4318/v1/logs"
CHUNK_SIZE   = 50 

def parse_kvp_line(line: str) -> dict:
    """Μετατρέπει μια γραμμή μορφής 'key=value' ή 'key="value"' σε dictionary."""
    # Αφαιρούμε το '1: ', '2: ' κλπ από την αρχή της γραμμής αν υπάρχει
    clean_line = re.sub(r'^\d+:\s*', '', line.strip())
    
    # Regular expression για να πιάνει key=value, ακόμα και αν το value έχει κενά μέσα σε εισαγωγικά
    pattern = r'(\w+)=("(?:\\"|[^"])*"|[^\s]+)'
    matches = re.findall(pattern, clean_line)
    
    log_dict = {}
    for key, value in matches:
        # Αφαιρούμε τα εισαγωγικά από τις τιμές αν υπάρχουν
        log_dict[key] = value.strip('"')
    return log_dict

def wrap_as_otlp(raw_logs: list) -> dict:
    records = []
    for log in raw_logs:
        body = json.dumps(log) if isinstance(log, dict) else str(log)
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
        elif fmt == "csv":
            reader = csv.DictReader(f)
            return [row for row in reader]
        elif fmt == "kvp":  # Νέα επιλογή για το συγκεκριμένο format
            return [parse_kvp_line(line) for line in f if line.strip()]
        else:  # plain text
            return [line.strip() for line in f if line.strip()]

def send(logs: list) -> None:
    total = len(logs)
    if total == 0:
        print("No logs found to send.")
        return
    batches = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
    print(f"Sending {total} log(s) in {batches} batch(es) of up to {CHUNK_SIZE}…")

    for i in range(0, total, CHUNK_SIZE):
        chunk = logs[i : i + CHUNK_SIZE]
        payload = wrap_as_otlp(chunk)
        try:
            resp = requests.post(PRODUCER_URL, json=payload, timeout=10)
            print(f"  Batch {i // CHUNK_SIZE + 1}/{batches}  →  "
                  f"status={resp.status_code}  accepted={resp.json().get('accepted', '?')}")
        except Exception as e:
            print(f"  Error sending batch: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to log file")
    parser.add_argument("--format", default="json",
                        choices=["json", "ndjson", "text", "csv", "kvp"], # Προσθήκη 'kvp'
                        help="File format (default: json)")
    args = parser.parse_args()

    logs = load_logs(args.file, args.format)
    send(logs)