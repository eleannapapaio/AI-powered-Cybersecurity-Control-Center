import hashlib
import json
import logging
import os
from langgraph.graph import END, START, StateGraph
from opensearchpy import OpenSearch, helpers
from pydantic import BaseModel, ValidationError
from typing_extensions import TypedDict

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class LogEntry(BaseModel):
    timestamp: str
    level: str
    message: str


class State(TypedDict):
    results: list
    valid: list
    invalid: list


def validate_node(state: State):
    total = len(state["results"])
    logger.info("[VALIDATE] Processing %d result(s)", total)

    valid, invalid = [], []
    for r in state["results"]:
        custom_id = r.get("custom_id", "<unknown>")
        try:
            content = r["response"]["body"]["choices"][0]["message"]["content"]
            data    = json.loads(content)
            entry   = LogEntry(**data)
            valid.append(entry.model_dump())
            logger.info(
                "[VALIDATE] OK  custom_id=%s  level=%s  ts=%s",
                custom_id, entry.level, entry.timestamp,
            )
        except (KeyError, IndexError) as e:
            logger.warning("[VALIDATE] SKIP  custom_id=%s  reason=missing_field  detail=%s", custom_id, e)
            invalid.append({"raw": r, "error": f"structure: {e}"})
        except json.JSONDecodeError as e:
            logger.warning("[VALIDATE] SKIP  custom_id=%s  reason=invalid_json  detail=%s", custom_id, e)
            invalid.append({"raw": r, "error": f"json: {e}"})
        except ValidationError as e:
            logger.warning("[VALIDATE] SKIP  custom_id=%s  reason=schema_mismatch  detail=%s", custom_id, e)
            invalid.append({"raw": r, "error": f"pydantic: {e}"})
        except Exception as e:
            logger.error("[VALIDATE] ERROR  custom_id=%s — %s", custom_id, e, exc_info=True)
            invalid.append({"raw": r, "error": str(e)})

    logger.info("[VALIDATE] Done  valid=%d  invalid=%d", len(valid), len(invalid))
    return {"valid": valid, "invalid": invalid}


def index_node(state: State):
    valid = state["valid"]
    logger.info("[INDEX] Indexing %d doc(s) to OpenSearch", len(valid))

    if not valid:
        logger.warning("[INDEX] No valid docs — skipping")
        return state

    os_host  = os.environ.get("OPENSEARCH_HOST", "opensearch")
    os_pass  = os.environ.get("OPENSEARCH_PASSWORD", "Pipeline@2024#")
    index    = "otel-logs-validated"

    try:
        client = OpenSearch(
            hosts=[{"host": os_host, "port": 9200}],
            http_auth=("admin", os_pass),
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False,
        )
        logger.info("[INDEX] Connected  host=%s  index=%s", os_host, index)
    except Exception as e:
        logger.error("[INDEX] Connection failed  host=%s — %s", os_host, e, exc_info=True)
        return state

    actions = [
        {
            "_index":  index,
            "_id":     hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest(),
            "_source": doc,
        }
        for doc in valid
    ]

    try:
        success, errors = helpers.bulk(client, actions, raise_on_error=False)
        if errors:
            logger.error("[INDEX] Bulk partial failure  success=%d  errors=%d", success, len(errors))
            for err in errors[:5]:
                logger.error("[INDEX] Bulk error detail: %s", err)
        else:
            logger.info("[INDEX] Bulk complete  indexed=%d", success)
    except Exception as e:
        logger.error("[INDEX] Bulk exception — %s", e, exc_info=True)

    return state


def build_graph():
    graph = StateGraph(State)
    graph.add_node("validate", validate_node)
    graph.add_node("index",    index_node)
    graph.add_edge(START, "validate")
    graph.add_edge("validate", "index")
    graph.add_edge("index", END)
    logger.info("[PIPELINE] Graph compiled  nodes=[validate → index]")
    return graph.compile()


pipeline = build_graph()


def validate_and_store(results):
    logger.info("[PIPELINE] Invoking pipeline  results=%d", len(results))
    state = {"results": results, "valid": [], "invalid": []}
    try:
        final = pipeline.invoke(state)
        logger.info(
            "[PIPELINE] Complete  valid=%d  invalid=%d",
            len(final.get("valid", [])), len(final.get("invalid", [])),
        )
    except Exception as e:
        logger.error("[PIPELINE] Fatal error — %s", e, exc_info=True)
        raise