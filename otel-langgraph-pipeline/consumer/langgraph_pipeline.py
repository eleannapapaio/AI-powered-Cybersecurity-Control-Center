"""
LangGraph Log Processing Pipeline  (v3 – Kafka/OTel consumer edition)
======================================================================

Changes from v2
---------------
- Receives a single batch (list[str]) from the Kafka consumer instead of
  managing multiple batches itself.  The consumer loop owns batch iteration.
- Output files written under /app/outputs/ (Docker volume mount).
- Invalid logs are ALSO published back to a Kafka dead-letter topic
  (raw-logs-dlq) so they can be reprocessed without touching the filesystem.
- `build_graph()` and `PipelineState` are the public API imported by consumer.py.

Flow (unchanged)
----------------
START → start_node → chat_prompt_node
          ↑               ↓ conditional_edge (Pydantic)
          │          valid → end_node → END
          └── tools_node ←── invalid (retry up to MAX_RETRIES)
"""

import json
import logging
import os
import hashlib
from typing import Optional

from pydantic import BaseModel, ValidationError
from typing_extensions import TypedDict

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from opensearchpy import OpenSearch, helpers

logger = logging.getLogger(__name__)

# ── output directory (mapped to host via Docker volume) ────────────────────────
OUTPUT_DIR     = os.environ.get("OUTPUT_DIR", "/app/outputs")
VALIDATED_FILE = os.path.join(OUTPUT_DIR, "validated_logs.txt")
INVALID_FILE   = os.path.join(OUTPUT_DIR, "invalid_logs_buffer.txt")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# 1.  Pydantic schema — mirrors the OTel-enriched log structure
# ──────────────────────────────────────────────────────────────────────────────

class ServiceInfo(BaseModel):
    name: str
    version: str
    env: str

class TraceInfo(BaseModel):
    trace_id: str
    span_id: str

class EventInfo(BaseModel):
    category: str
    action: str
    duration_ms: int

class UserInfo(BaseModel):
    id: str
    ip: str

class ErrorInfo(BaseModel):
    code: str
    stack_trace: str

class MetadataInfo(BaseModel):
    region: str
    container_id: str

class LogEntry(BaseModel):
    timestamp: str
    level: str
    message: str
    service: ServiceInfo
    trace: TraceInfo
    event: EventInfo
    user: UserInfo
    error: Optional[ErrorInfo] = None
    metadata: MetadataInfo


# ──────────────────────────────────────────────────────────────────────────────
# 2.  Graph state
#     NOTE: all_batches always contains exactly ONE batch (sent by the consumer).
#     batch_index starts at 0 and ends at 1 → the graph runs once per Kafka msg.
# ──────────────────────────────────────────────────────────────────────────────

class PipelineState(TypedDict):
    # ── input ──────────────────────────────────────────────────────────────
    all_batches:  list[list[str]]    # [[log1, log2, ...]] – always 1 batch
    batch_index:  int

    # ── per-batch working state ────────────────────────────────────────────
    current_batch:    list[str]
    messages:         list
    llm_response_raw: Optional[str]

    # ── retry tracking ─────────────────────────────────────────────────────
    retry_count:      int
    pending_raw_logs: list[str]      # subset still awaiting valid parse

    # ── outputs ────────────────────────────────────────────────────────────
    validated_entries: list[dict]
    invalid_buffer:    list[dict]    # unrecoverable after MAX_RETRIES

    # ── kafka meta (injected by consumer, passed through for DLQ routing) ──
    kafka_topic:      str
    kafka_partition:  int
    kafka_offset:     int


# ──────────────────────────────────────────────────────────────────────────────
# 3.  LLM + system prompt
# ──────────────────────────────────────────────────────────────────────────────

MAX_RETRIES = 3

llm = ChatOpenAI(
    model=os.environ.get("OPENAI_MODEL", "gpt-4.1"),
    temperature=0,
)

SYSTEM_PROMPT = SystemMessage(content="""
You are a log-parsing assistant operating inside a real-time data pipeline.

You receive raw OpenTelemetry log strings (OTLP JSON or unstructured text).
For EACH log in the input array, extract its fields and return a JSON object
that STRICTLY matches this schema:

{
  "timestamp": "<ISO-8601 string>",
  "level":     "<log level>",
  "message":   "<human-readable message>",
  "service":   {"name": "...", "version": "...", "env": "..."},
  "trace":     {"trace_id": "...", "span_id": "..."},
  "event":     {"category": "...", "action": "...", "duration_ms": <int>},
  "user":      {"id": "...", "ip": "..."},
  "error":     {"code": "...", "stack_trace": "..."},
  "metadata":  {"region": "...", "container_id": "..."}
}

Rules:
- Omit the "error" key entirely if no error is present in the source log.
- Infer missing fields from context where reasonable; use "unknown" as a last resort.
- Return ONLY a JSON array — one object per input log, same order.
- No markdown fences, no explanation text, no extra keys.
""")


# ──────────────────────────────────────────────────────────────────────────────
# 4.  Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _clean(raw: str) -> str:
    return (
        raw.strip()
           .removeprefix("```json")
           .removeprefix("```")
           .removesuffix("```")
           .strip()
    )


def _validate_entries(
    parsed_list: list[dict],
) -> tuple[list[dict], list[tuple[dict, str]]]:
    valid, invalid = [], []
    for item in parsed_list:
        try:
            entry = LogEntry(**item)
            valid.append(entry.model_dump())
        except (ValidationError, Exception) as exc:
            invalid.append((item, str(exc)))
    return valid, invalid


def _append_file(path: str, records: list[dict]) -> None:
    with open(path, "a") as f:
        for rec in records:
            f.write(json.dumps(rec, indent=2) + "\n---\n")


# ──────────────────────────────────────────────────────────────────────────────
# 5.  Nodes
# ──────────────────────────────────────────────────────────────────────────────
os_client = OpenSearch(
    hosts=[{
        "host": os.environ.get("OPENSEARCH_HOST", "localhost"),
        "port": 9200
    }],
    http_auth=("admin", os.environ.get("OPENSEARCH_PASSWORD", "Pipeline@2024#")),
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False,
)

INDEX_NAME = "otel-logs-validated"


def start_node(state: PipelineState) -> PipelineState:
    idx   = state.get("batch_index", 0)
    batch = state["all_batches"][idx]
    logger.info(
        "[START] batch=%d/%d logs=%d partition=%d offset=%d",
        idx + 1, len(state["all_batches"]), len(batch),
        state.get("kafka_partition", -1), state.get("kafka_offset", -1),
    )
    return {
        **state,
        "current_batch":    batch,
        "pending_raw_logs": batch,
        "messages":         [SYSTEM_PROMPT],
        "llm_response_raw": None,
        "retry_count":      0,
    }


def chat_prompt_node(state: PipelineState) -> PipelineState:
    pending  = state["pending_raw_logs"]
    messages = list(state["messages"])
    messages.append(HumanMessage(
        content=(
            f"Parse the following {len(pending)} raw OTel log(s) "
            f"and return a JSON array:\n\n"
            f"{json.dumps(pending, indent=2)}"
        )
    ))
    logger.info("[LLM] sending %d log(s), retry=%d", len(pending), state["retry_count"])
    response = llm.invoke(messages)
    messages.append(response)
    return {**state, "messages": messages, "llm_response_raw": response.content}


def conditional_edge_fn(state: PipelineState) -> str:
    raw = _clean(state.get("llm_response_raw", ""))
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            raise ValueError("expected JSON array")
    except Exception as exc:
        logger.warning("[VALIDATION] parse error: %s", exc)
        return "end_node" if state["retry_count"] >= MAX_RETRIES else "tools_node"

    valid, invalid = _validate_entries(parsed)
    logger.info("[VALIDATION] valid=%d invalid=%d", len(valid), len(invalid))

    if not invalid:
        return "end_node"
    if state["retry_count"] >= MAX_RETRIES:
        logger.warning("[VALIDATION] max retries → %d log(s) to buffer", len(invalid))
        return "end_node"
    return "tools_node"


def tools_node(state: PipelineState) -> PipelineState:
    raw     = _clean(state.get("llm_response_raw", ""))
    pending = state["pending_raw_logs"]

    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            raise ValueError
    except Exception:
        parsed = [{} for _ in pending]

    _, invalid_pairs = _validate_entries(parsed)

    # Identify failing positions by index
    invalid_indices: set[int] = set()
    for bad_dict, _ in invalid_pairs:
        try:
            invalid_indices.add(parsed.index(bad_dict))
        except ValueError:
            pass
    if not invalid_indices:
        invalid_indices = set(range(len(pending)))

    still_pending = [pending[i] for i in sorted(invalid_indices)]
    error_details = "\n".join(
        f"  Log {i}: {err}" for i, (_, err) in enumerate(invalid_pairs)
    )

    messages = list(state["messages"])
    messages.append(HumanMessage(content=(
        f"The following {len(still_pending)} log(s) failed Pydantic validation.\n\n"
        f"Errors:\n{error_details}\n\n"
        f"Re-parse ONLY these logs and return a corrected JSON array "
        f"(one object per log, same order):\n\n"
        f"{json.dumps(still_pending, indent=2)}\n\n"
        "Return ONLY the JSON array — no markdown, no explanation."
    )))

    logger.info("[TOOLS] re-queuing %d log(s), retry=%d/%d",
                len(still_pending), state["retry_count"] + 1, MAX_RETRIES)
    return {
        **state,
        "messages":         messages,
        "llm_response_raw": None,
        "pending_raw_logs": still_pending,
        "retry_count":      state["retry_count"] + 1,
    }


def end_node(state: PipelineState) -> PipelineState:
    raw        = _clean(state.get("llm_response_raw", "") or "")
    validated  = list(state.get("validated_entries", []))
    inv_buffer = list(state.get("invalid_buffer", []))

    try:
        parsed = json.loads(raw) if raw else []
        if not isinstance(parsed, list):
            parsed = []
    except Exception:
        parsed = []

    valid, invalid_pairs = _validate_entries(parsed)
    validated.extend(valid)

    if valid:
        bulk_actions = [
            {
                "_index": INDEX_NAME,
                "_id": hashlib.sha256(
                    json.dumps(doc, sort_keys=True).encode()
                ).hexdigest(),   # deterministic ID — same log = same ID = no duplicate
                "_source": doc,
            }
            for doc in valid
        ]
        try:
            success, failed = helpers.bulk(
                os_client,
                bulk_actions,
                raise_on_error=False,   # don't crash if a doc already exists
            )
            logger.info("[OPENSEARCH] Bulk insert: %d new docs, %d skipped/failed",
                        success, len(failed) if failed else 0)
        except Exception as e:
            logger.error("[OPENSEARCH] Bulk insert failed: %e", e)

    # Persist valid entries (append so multiple Kafka messages accumulate)
    _append_file(VALIDATED_FILE, valid)

    # Unrecoverable entries → local buffer file + in-state buffer for DLQ
    pending = state.get("pending_raw_logs", [])
    new_invalid: list[dict] = []
    for i, (bad_dict, error_str) in enumerate(invalid_pairs):
        raw_log = pending[i] if i < len(pending) else json.dumps(bad_dict)
        record  = {
            "raw_log":          raw_log,
            "last_llm_output":  bad_dict,
            "validation_error": error_str,
            "batch_index":      state["batch_index"],
            "kafka_topic":      state.get("kafka_topic", ""),
            "kafka_partition":  state.get("kafka_partition", -1),
            "kafka_offset":     state.get("kafka_offset", -1),
        }
        new_invalid.append(record)
        logger.warning("[BUFFER] invalid log → DLQ: %s…", raw_log[:80])

    inv_buffer.extend(new_invalid)
    if new_invalid:
        _append_file(INVALID_FILE, new_invalid)

    logger.info("[END] validated=%d buffered=%d", len(validated), len(inv_buffer))
    return {
        **state,
        "validated_entries": validated,
        "invalid_buffer":    inv_buffer,
        "batch_index":       state["batch_index"] + 1,
    }


def should_continue(state: PipelineState) -> str:
    if state["batch_index"] < len(state["all_batches"]):
        return "start_node"
    return END


# ──────────────────────────────────────────────────────────────────────────────
# 6.  Graph builder  (public API used by consumer.py)
# ──────────────────────────────────────────────────────────────────────────────

def build_graph():
    g = StateGraph(PipelineState)

    g.add_node("start_node",       start_node)
    g.add_node("chat_prompt_node", chat_prompt_node)
    g.add_node("tools_node",       tools_node)
    g.add_node("end_node",         end_node)

    g.add_edge(START,            "start_node")
    g.add_edge("start_node",     "chat_prompt_node")
    g.add_conditional_edges(
        "chat_prompt_node",
        conditional_edge_fn,
        {"end_node": "end_node", "tools_node": "tools_node"},
    )
    g.add_edge("tools_node",     "chat_prompt_node")
    g.add_conditional_edges(
        "end_node",
        should_continue,
        {"start_node": "start_node", END: END},
    )

    return g.compile()