import argparse
import json
import time
import socket
import csv
import re
from datetime import datetime, timezone

# ── Fluent Bit syslog UDP targets ────────────────────────────────────────────
FLUENT_BIT_HOST = "localhost"

# Port → tag mapping (mirrors fluent-bit.conf)
DEVICE_PORTS = {
    "generic":             514,
    "cisco_asa":          1514,
    "cisco_ios":          1515,
    "cisco_firepower":    1516,
    "cisco_stealthwatch": 1517,
    "fortigate":          1518,
    "fortiweb":           1519,
    "paloalto_traffic":   1520,
    "paloalto_threat":    1521,
    "juniper_srx":        1522,
    "checkpoint":         1523,
    "sophos_xg":          1524,
    "pfsense":            1525,
    "watchguard":         1526,
    "untangle":           1527,
    "suricata":           1528,
    "zeek":               1529,
    "iptables":           1530,
}

CHUNK_SIZE = 50
# Delay between datagrams (seconds). Raise if Fluent Bit drops packets.
SEND_DELAY = 0.01


# ── Syslog helpers ────────────────────────────────────────────────────────────

def make_rfc3164(message: str, hostname: str = "test-host",
                 app_name: str = "send_logs") -> bytes:
    """Wrap a message string in an RFC 3164 syslog frame (facility=1, severity=6 → INFO)."""
    pri = (1 << 3) | 6          # facility=user(1), severity=info(6)
    ts  = datetime.now().strftime("%b %d %H:%M:%S")   # "Jan  1 00:00:00"
    # RFC 3164: <PRI>TIMESTAMP HOSTNAME TAG: MSG
    frame = f"<{pri}>{ts} {hostname} {app_name}: {message}"
    return frame.encode("utf-8", errors="replace")


def log_to_message(log) -> str:
    """Convert a log entry (dict, str, …) to a single-line string."""
    if isinstance(log, dict):
        return json.dumps(log, ensure_ascii=False)
    return str(log)


# ── File loaders ──────────────────────────────────────────────────────────────

def parse_kvp_line(line: str) -> dict:
    clean_line = re.sub(r'^\d+:\s*', '', line.strip())
    pattern    = r'(\w+)=("(?:\\"|[^"])*"|[^\s]+)'
    matches    = re.findall(pattern, clean_line)
    return {k: v.strip('"') for k, v in matches}


def load_logs(filepath: str, fmt: str) -> list:
    with open(filepath, "r", encoding="utf-8") as f:
        if fmt == "json":
            data = json.load(f)
            return data if isinstance(data, list) else [data]
        elif fmt == "ndjson":
            return [json.loads(line) for line in f if line.strip()]
        elif fmt == "csv":
            return list(csv.DictReader(f))
        elif fmt == "kvp":
            return [parse_kvp_line(line) for line in f if line.strip()]
        else:   # plain text
            return [line.strip() for line in f if line.strip()]


# ── Sender ────────────────────────────────────────────────────────────────────

def send(logs: list, host: str, port: int) -> None:
    total = len(logs)
    if total == 0:
        print("No logs found to send.")
        return

    batches = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
    print(f"Sending {total} log(s) → {host}:{port} (UDP syslog) "
          f"in {batches} batch(es) of up to {CHUNK_SIZE}…")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sent = 0
    errors = 0

    try:
        for i, log in enumerate(logs):
            message = log_to_message(log)
            datagram = make_rfc3164(message)
            try:
                sock.sendto(datagram, (host, port))
                sent += 1
            except Exception as e:
                print(f"  [!] Error on log {i + 1}: {e}")
                errors += 1
            # Print progress at batch boundaries
            if (i + 1) % CHUNK_SIZE == 0 or (i + 1) == total:
                print(f"  Sent {i + 1}/{total}  (errors so far: {errors})")
            time.sleep(SEND_DELAY)
    finally:
        sock.close()

    print(f"\nDone. {sent} sent, {errors} errors.")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Replay logs from a file into Fluent Bit via UDP syslog."
    )
    parser.add_argument("--file",   required=True,
                        help="Path to the log file")
    parser.add_argument("--format", default="json",
                        choices=["json", "ndjson", "text", "csv", "kvp"],
                        help="File format (default: json)")
    parser.add_argument("--device", default="generic",
                        choices=list(DEVICE_PORTS.keys()),
                        help="Target device type → selects the Fluent Bit UDP port "
                             "(default: generic / port 514)")
    parser.add_argument("--host",   default=FLUENT_BIT_HOST,
                        help=f"Fluent Bit host (default: {FLUENT_BIT_HOST})")
    parser.add_argument("--port",   type=int, default=None,
                        help="Override UDP port (ignores --device port mapping)")
    parser.add_argument("--delay",  type=float, default=SEND_DELAY,
                        help=f"Seconds between datagrams (default: {SEND_DELAY})")
    args = parser.parse_args()

    port = args.port if args.port is not None else DEVICE_PORTS[args.device]

    print(f"Device type : {args.device}")
    print(f"Target      : {args.host}:{port} (UDP)")
    print(f"File        : {args.file}  [{args.format}]")
    print()

    SEND_DELAY = args.delay   # allow runtime override
    logs = load_logs(args.file, args.format)
    send(logs, args.host, port)