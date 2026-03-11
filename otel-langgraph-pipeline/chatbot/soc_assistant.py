"""
SOC Assistant — LangGraph Chatbot
===================================

Graph
─────

  START
    │
    ▼
┌─────────────┐   intent = "off_topic"
│ intent_node │──────────────────────────► off_topic_node ──► END
│  (LLM  1)   │                              (LLM  2a)
└──────┬──────┘
       │  intent ∈ {retrieval, correlation, remediation}
       ▼
┌──────────────────┐
│  query_gen_node  │  (LLM 2b)  generate OpenSearch DSL
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ opensearch_node  │  execute query — NO LLM
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  analyst_node    │  (LLM 3)  reason over results → human answer
└────────┬─────────┘
         │
        END

Intent types
────────────
  retrieval   – user wants to list/view specific log events
  correlation – user wants to find patterns or detect attacks
  remediation – user wants advice on how to block or fix an issue
  off_topic   – question NOT related to SOC / infra → short-circuit, no OpenSearch

Every LLM call prints a clearly labelled block to the terminal.
"""

import json
import logging
import os
from typing import Optional

from opensearchpy import OpenSearch
from typing_extensions import TypedDict

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph

# ── logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ── terminal printer ───────────────────────────────────────────────────────────

def _print_llm(node: str, prompt_summary: str, response: str) -> None:
    """Pretty-print every LLM call to the terminal."""
    width = 72
    sep   = "═" * width
    thin  = "─" * width
    print(f"\n{sep}")
    print(f"  🤖  LLM CALL  ·  {node}")
    print(thin)
    print(f"  ✉   PROMPT   : {prompt_summary[:200]}")
    print(thin)
    print(f"  ✅  RESPONSE  :")
    for line in response.splitlines():
        print(f"       {line}")
    print(f"{sep}\n")


def _print_opensearch(query: dict, result_count: int) -> None:
    """Print the OpenSearch query and result count."""
    width = 72
    sep   = "═" * width
    thin  = "─" * width
    print(f"\n{sep}")
    print(f"  🔎  OPENSEARCH NODE  (no LLM)")
    print(thin)
    print(f"  📋  QUERY    : {json.dumps(query)[:300]}")
    print(thin)
    print(f"  📦  RESULTS  : {result_count} document(s) returned")
    print(f"{sep}\n")


# ── OpenSearch client ──────────────────────────────────────────────────────────
os_client = OpenSearch(
    hosts=[{"host": os.environ.get("OPENSEARCH_HOST", "localhost"), "port": 9200}],
    http_auth=("admin", os.environ.get("OPENSEARCH_PASSWORD", "Pipeline@2024#")),
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False,
)
INDEX_NAME = "otel-logs-validated"

# ── LLM ───────────────────────────────────────────────────────────────────────
llm = ChatOpenAI(
    model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
    temperature=0,
)


# ── Graph state ────────────────────────────────────────────────────────────────

class ChatState(TypedDict):
    session_id:    str
    chat_history:  list             # [{role, content}, ...]
    user_question: str

    intent:        Optional[str]    # retrieval | correlation | remediation | off_topic
    filters:       Optional[dict]
    os_query:      Optional[dict]
    os_results:    Optional[list[dict]]
    answer:        Optional[str]


# ── System prompts ─────────────────────────────────────────────────────────────

INTENT_SYSTEM = SystemMessage(content="""
You are a SOC (Security Operations Center) assistant router.

Classify the user's question into EXACTLY ONE of these intents:

  retrieval   — user wants to SEE or LIST specific log events from infrastructure
  correlation — user wants to FIND PATTERNS, detect attacks, or correlate events
  remediation — user wants ADVICE on how to fix, block, or respond to a security issue
  off_topic   — the question is NOT related to infrastructure, logs, security, or SOC work
                (e.g. general knowledge, coding help, weather, jokes, maths, etc.)

For intents retrieval / correlation / remediation ONLY, also extract these filters:
  level      : ERROR | WARN | INFO | DEBUG | CRITICAL  (null if not mentioned)
  service    : service name if mentioned               (null if not mentioned)
  time_range : last_hour | last_day | last_week | last_month | all_time   (default: all_time)
              Use all_time if the user says "any", "all", "ever", or does not mention a time period.
  keyword    : any specific search term — error code, IP, user ID, action, country, region,
               service name, HTTP method, browser, OS, or any word from the message field.
               Extract this whenever the user mentions a specific word to search for.

Respond ONLY with a valid JSON object — no markdown, no explanation:
{
  "intent":  "<retrieval|correlation|remediation|off_topic>",
  "filters": {
    "level":      "<level or null>",
    "service":    "<service name or null>",
    "time_range": "<last_hour|last_day|last_week|last_month|all_time>",
    "keyword":    "<keyword or null>"
  }
}

For off_topic, still return the full JSON but filters values can all be null.
""")

QUERY_GEN_SYSTEM = SystemMessage(content="""
You are an OpenSearch DSL expert for a SOC log analytics platform.

The index `otel-logs-validated` contains these fields:
  timestamp              (date, ISO-8601)
  level                  (keyword: ERROR, WARN, INFO, DEBUG)
  message                (text)
  service.name           (keyword)
  service.version        (keyword)
  service.env            (keyword)
  trace.trace_id         (keyword)
  trace.span_id          (keyword)
  event.category         (keyword)
  event.action           (keyword)
  event.duration_ms      (integer)
  user.id                (keyword)
  user.ip                (keyword)
  error.code             (keyword)
  error.stack_trace      (text)
  metadata.region        (keyword)
  metadata.container_id  (keyword)

Query rules:
  retrieval   → sort by timestamp desc, size 20
  correlation → aggregations grouping by service.name, level, event.action; size 0
  remediation → fetch specific event details, size 5

Return ONLY the raw JSON query body that goes into the OpenSearch `body` parameter.
No markdown fences, no explanation, no extra keys.
""")

OFF_TOPIC_SYSTEM = SystemMessage(content="""
You are a SOC (Security Operations Center) assistant specialized exclusively in
infrastructure monitoring, log analysis, and security operations.

The user has asked something outside your area of expertise.
Politely let them know in 2-3 sentences that you are specialized for SOC/infrastructure
topics, and briefly list 2-3 example questions you CAN help with.
""")

ANALYST_SYSTEM = SystemMessage(content="""
You are an expert SOC (Security Operations Center) analyst assistant.

You will receive an intent and log data. Answer ONLY for the detected intent — never output sections for the others.

If intent = retrieval:
  Present the matching log events as a markdown table.
  Columns: Timestamp | Level | Service | Message | User IP
  No correlation analysis. No remediation steps.

If intent = correlation:
  Analyse the log data for patterns, anomalies, repeated failures, or attack indicators.
  Group findings by service, IP, or error type where useful.
  No raw event listing. No remediation steps.

If intent = remediation:
  Give specific numbered steps to fix or block the issue, referencing the exact IPs,
  services, and error codes found in the logs.
  No raw event listing. No correlation analysis.

Rules:
  - ONE focused answer for the given intent — never output all three sections.
  - Always cite specific data from the logs (timestamps, IPs, error codes, service names).
  - Be direct and professional — this is a security operations context.
  - If no relevant logs were found, say so in one sentence and suggest what to check.
  - Use markdown but keep it focused — a single clear section.
""")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _history_messages(chat_history: list) -> list:
    """Convert stored chat history to LangChain message objects (last 10 turns)."""
    msgs = []
    for e in chat_history[-10:]:
        if e["role"] == "user":
            msgs.append(HumanMessage(content=e["content"]))
        else:
            msgs.append(AIMessage(content=e["content"]))
    return msgs


def _time_filter(time_range: str) -> dict:
    if time_range == "all_time":
        return {"match_all": {}}   # no date restriction — search entire index
    gt = {
        "last_hour":  "now-1h/h",
        "last_day":   "now-1d/d",
        "last_week":  "now-7d/d",
        "last_month": "now-30d/d",
    }
    gte = gt.get(time_range, "now-7d/d")
    return {"range": {"timestamp": {"gte": gte}}}


def _clean(raw: str) -> str:
    return raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()


# ── Nodes ─────────────────────────────────────────────────────────────────────

def intent_node(state: ChatState) -> ChatState:
    """
    LLM CALL 1 — Classify intent and extract filters.
    Printed to terminal.
    """
    history  = _history_messages(state.get("chat_history", []))
    messages = [INTENT_SYSTEM] + history + [HumanMessage(content=state["user_question"])]

    response = llm.invoke(messages)
    raw      = _clean(response.content)

    _print_llm(
        node="intent_node  (LLM CALL 1)",
        prompt_summary=f"Classify question: \"{state['user_question']}\"",
        response=raw,
    )

    try:
        parsed  = json.loads(raw)
        intent  = parsed.get("intent", "retrieval")
        filters = parsed.get("filters", {})
    except Exception:
        logger.warning("[INTENT] Failed to parse JSON — defaulting to retrieval")
        intent  = "retrieval"
        filters = {"time_range": "last_day"}

    logger.info("[INTENT] → %s   filters=%s", intent, filters)
    return {**state, "intent": intent, "filters": filters}


def route_after_intent(state: ChatState) -> str:
    """Conditional edge: short-circuit off_topic, continue pipeline for SOC intents."""
    if state["intent"] == "off_topic":
        logger.info("[ROUTE] off_topic → short-circuit to off_topic_node")
        return "off_topic_node"
    logger.info("[ROUTE] %s → continuing to query_gen_node", state["intent"])
    return "query_gen_node"


def off_topic_node(state: ChatState) -> ChatState:
    """
    LLM CALL 2a — Polite redirect for off-topic questions.
    SHORT-CIRCUIT: skips OpenSearch and analyst entirely.
    Printed to terminal.
    """
    history  = _history_messages(state.get("chat_history", []))
    messages = [OFF_TOPIC_SYSTEM] + history + [HumanMessage(content=state["user_question"])]

    response = llm.invoke(messages)
    answer   = response.content

    _print_llm(
        node="off_topic_node  (LLM CALL 2a — SHORT CIRCUIT)",
        prompt_summary=f"Off-topic question: \"{state['user_question']}\"",
        response=answer,
    )

    chat_history = list(state.get("chat_history", []))
    chat_history.append({"role": "user",      "content": state["user_question"]})
    chat_history.append({"role": "assistant", "content": answer})

    logger.info("[OFF_TOPIC] Done — %d chars, skipping OpenSearch + analyst", len(answer))
    return {**state, "answer": answer, "chat_history": chat_history}


def query_gen_node(state: ChatState) -> ChatState:
    """
    LLM CALL 2b — Generate OpenSearch DSL query from intent + filters.
    Printed to terminal.
    """
    intent  = state["intent"]
    filters = state.get("filters", {})
    context = (
        f"Intent: {intent}\n"
        f"Filters: {json.dumps(filters)}\n"
        f"User question: {state['user_question']}"
    )

    messages = [QUERY_GEN_SYSTEM, HumanMessage(content=context)]
    response = llm.invoke(messages)
    raw      = _clean(response.content)

    _print_llm(
        node="query_gen_node  (LLM CALL 2b)",
        prompt_summary=f"Generate DSL  intent={intent}  filters={filters}",
        response=raw,
    )

    try:
        query = json.loads(raw)
    except Exception:
        logger.warning("[QUERY_GEN] Failed to parse DSL — using safe default query")
        query = {}

    # Always rebuild as a clean bool/must query.
    # The LLM sometimes returns flat {"query": {"range": ...}} or mixes
    # range + bool at the same level — both produce 0 results in OpenSearch.
    time_range  = filters.get("time_range", "last_week")
    time_clause = _time_filter(time_range)
    must = [time_clause]   # time filter is always first

    # Salvage any extra clauses the LLM added (skip stray range clauses)
    llm_query = query.get("query", {})
    for key in ("match", "term", "multi_match", "match_phrase"):
        if key in llm_query:
            must.append({key: llm_query[key]})
    if "bool" in llm_query:
        for clause in llm_query["bool"].get("must", []):
            if "range" not in clause:
                must.append(clause)

    # Hard-inject explicit filters from the intent classifier
    if filters.get("level"):
        must.append({"term": {"level": filters["level"].upper()}})
    if filters.get("service"):
        must.append({"match": {"service.name": filters["service"]}})
    if filters.get("keyword"):
        must.append({"multi_match": {
            "query":  filters["keyword"],
            "fields": ["message", "error.code", "user.ip", "user.id", "event.action", "metadata.region", "service.name", "event.category"],
        }})

    final_query = {
        "query": {"bool": {"must": must}},
        "sort":  [{"timestamp": {"order": "desc"}}],
        "size":  query.get("size", 20),
    }
    if "aggs" in query:
        final_query["aggs"] = query["aggs"]
    if "aggregations" in query:
        final_query["aggregations"] = query["aggregations"]

    logger.info("[QUERY_GEN] Final DSL: %s", json.dumps(final_query)[:400])
    return {**state, "os_query": final_query}


def opensearch_node(state: ChatState) -> ChatState:
    """
    Execute the OpenSearch DSL query — NO LLM.
    Printed to terminal.
    """
    query = state["os_query"]
    try:
        resp    = os_client.search(index=INDEX_NAME, body=query)
        hits    = [h["_source"] for h in resp["hits"]["hits"]]
        aggs    = resp.get("aggregations", {})
        results = hits + ([{"_aggregations": aggs}] if aggs else [])
        logger.info("[OPENSEARCH] %d hit(s) returned", len(hits))
    except Exception as exc:
        logger.error("[OPENSEARCH] Query failed: %s", exc)
        results = []

    _print_opensearch(query, len(results))
    return {**state, "os_results": results}


def analyst_node(state: ChatState) -> ChatState:
    """
    LLM CALL 3 — Reason over OpenSearch results, produce human-language answer.
    Printed to terminal.
    """
    results  = state.get("os_results", [])
    intent   = state["intent"]
    question = state["user_question"]
    history  = _history_messages(state.get("chat_history", []))

    results_text = json.dumps(results[:20], indent=2) if results else "No logs found."

    context_msg = HumanMessage(content=(
        f"User question: {question}\n\n"
        f"INTENT: {intent.upper()} — produce ONLY a {intent} answer. "
        f"Do NOT include sections for other intents.\n\n"
        f"Log data from OpenSearch ({len(results)} result(s)):\n"
        f"```json\n{results_text}\n```\n\n"
        f"Provide a focused {intent} answer based strictly on the data above."
    ))

    messages = [ANALYST_SYSTEM] + history + [context_msg]
    response = llm.invoke(messages)
    answer   = response.content

    _print_llm(
        node="analyst_node  (LLM CALL 3)",
        prompt_summary=f"Analyze {len(results)} log(s)  intent={intent}",
        response=answer,
    )

    chat_history = list(state.get("chat_history", []))
    chat_history.append({"role": "user",      "content": question})
    chat_history.append({"role": "assistant", "content": answer})

    logger.info("[ANALYST] Answer generated — %d chars", len(answer))
    return {**state, "answer": answer, "chat_history": chat_history}


# ── Graph builder ─────────────────────────────────────────────────────────────

def build_soc_graph():
    g = StateGraph(ChatState)

    g.add_node("intent_node",     intent_node)
    g.add_node("off_topic_node",  off_topic_node)
    g.add_node("query_gen_node",  query_gen_node)
    g.add_node("opensearch_node", opensearch_node)
    g.add_node("analyst_node",    analyst_node)

    # START → intent classification (always)
    g.add_edge(START, "intent_node")

    # Branch: off_topic short-circuits; SOC intents continue
    g.add_conditional_edges(
        "intent_node",
        route_after_intent,
        {
            "off_topic_node": "off_topic_node",
            "query_gen_node": "query_gen_node",
        },
    )

    # off_topic path ends immediately (no OpenSearch, no analyst)
    g.add_edge("off_topic_node",  END)

    # SOC pipeline: query → search → analyse → end
    g.add_edge("query_gen_node",  "opensearch_node")
    g.add_edge("opensearch_node", "analyst_node")
    g.add_edge("analyst_node",    END)

    return g.compile()


# ── Compiled pipeline (imported by app.py) ────────────────────────────────────
soc_pipeline = build_soc_graph()