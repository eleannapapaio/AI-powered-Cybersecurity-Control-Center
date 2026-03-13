import json
import hashlib
import logging
import os

from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel, ValidationError
from typing_extensions import TypedDict

from opensearchpy import OpenSearch, helpers

logger = logging.getLogger(__name__)


class LogEntry(BaseModel):

    timestamp: str
    level: str
    message: str


class State(TypedDict):

    results: list
    valid: list
    invalid: list


def validate_node(state: State):

    valid = []
    invalid = []

    for r in state["results"]:

        try:

            content = r["response"]["body"]["choices"][0]["message"]["content"]

            data = json.loads(content)

            entry = LogEntry(**data)

            valid.append(entry.model_dump())

        except Exception as e:

            invalid.append({"raw": r, "error": str(e)})

    return {"valid": valid, "invalid": invalid}


def index_node(state: State):

    valid = state["valid"]

    if not valid:
        return state

    # Fixed: use SSL + correct password + correct index name
    client = OpenSearch(
        hosts=[{"host": os.environ.get("OPENSEARCH_HOST", "opensearch"), "port": 9200}],
        http_auth=("admin", os.environ.get("OPENSEARCH_PASSWORD", "Pipeline@2024#")),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False,
    )

    actions = []

    for doc in valid:

        actions.append({
            "_index": "otel-logs-validated",   # fixed: was "otel-logs"
            "_id": hashlib.sha256(json.dumps(doc).encode()).hexdigest(),
            "_source": doc
        })

    helpers.bulk(client, actions)

    return state


def build_graph():

    graph = StateGraph(State)

    graph.add_node("validate", validate_node)
    graph.add_node("index", index_node)

    graph.add_edge(START, "validate")
    graph.add_edge("validate", "index")
    graph.add_edge("index", END)

    return graph.compile()


pipeline = build_graph()


def validate_and_store(results):

    state = {
        "results": results,
        "valid": [],
        "invalid": []
    }

    pipeline.invoke(state)