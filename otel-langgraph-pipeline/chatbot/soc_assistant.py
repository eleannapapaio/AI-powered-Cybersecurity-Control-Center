"""
SOC Assistant — LangGraph Chatbot
===================================

Graph
─────

  START
    │
    ▼
┌─────────────┐   intent = "off_topic"      ──► off_topic_node ──► END
│ intent_node │                                  (LLM  2a)
│  (LLM  1)   │   intent = "context_aware"  ──► analyst_node  ──► END
└──────┬──────┘                                  (LLM  3, no OpenSearch)
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
  retrieval      – user wants to list/view specific log events
  correlation    – user wants to find patterns or detect attacks
  remediation    – user wants advice on how to block or fix an issue
  context_aware  – follow-up referencing already-shown results → skip OpenSearch
  off_topic      – question NOT related to SOC / infra → short-circuit entirely

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
    model=os.environ.get("OPENAI_MODEL", "gpt-4.1"),
    temperature=0,
)


# ── Graph state ────────────────────────────────────────────────────────────────

class ChatState(TypedDict):
    session_id:    str
    chat_history:  list             # [{role, content}, ...]
    user_question: str

    intent:        Optional[str]    # retrieval | correlation | remediation | context_aware | off_topic
    filters:       Optional[dict]
    os_query:      Optional[dict]
    os_results:    Optional[list[dict]]
    answer:        Optional[str]


# ── System prompts ─────────────────────────────────────────────────────────────

INTENT_SYSTEM = SystemMessage(content="""
You are the routing layer of a SOC (Security Operations Center) log-analysis assistant.

## Task
Classify the user's question into exactly one intent and extract search filters.

## Intents

| Intent | When to use |
|---|---|
| retrieval | User wants to VIEW or LIST specific raw log events (e.g. "show me errors", "list requests from Germany") |
| correlation | User wants PATTERN ANALYSIS, anomaly detection, attack identification, or cross-service trends |
| remediation | User wants ACTIONABLE FIX/BLOCK steps for a security or infrastructure problem |
| context_aware | User is asking a FOLLOW-UP about results ALREADY shown in the conversation — no new search needed |
| off_topic | Question has NO connection to logs, infra, or security (weather, coding help, maths, jokes, etc.) |

## CRITICAL: When to use context_aware
Use `context_aware` when the user is drilling into data already retrieved. Signs:
- Uses pronouns referring to previous results: "they", "those", "it", "them", "these"
- References position: "the first one", "the second log", "the last entry"
- Asks to elaborate on already-shown data: "tell me more", "explain that", "what does it mean"
- Asks to count, summarize, or reformat already-shown data: "how many?", "summarize", "group by IP"
- No new search term — the user is working with what is already on screen

Use a fresh retrieval/correlation/remediation (NOT context_aware) when:
- The user introduces a new filter, keyword, service, IP, or region
- The user asks about something not yet fetched (e.g. "now show me WARN logs" after seeing ERROR logs)
- There is no prior assistant answer in the conversation

## Filter extraction rules
For retrieval / correlation / remediation ONLY (not for context_aware or off_topic):

- level: One of ERROR, WARN, INFO, DEBUG, CRITICAL — null if not specified
- service: Exact service name if mentioned — null otherwise
- keyword: Any specific term to search — IP address, user ID, error code, HTTP method,
  country, region, action, browser, OS, or any notable word from the message.
  Prefer the most specific term the user mentions. null if none.
- date_filter: Extract ONLY when the user explicitly mentions a specific date or date range.
  null if no date is mentioned — do NOT default to any date restriction.

  Format rules:
  - Specific day (e.g. "from 1/1/2023", "on January 1 2023"):
      {"gte": "2023-01-01T00:00:00", "lte": "2023-01-01T23:59:59"}
  - Date range (e.g. "between Jan 1 and Jan 5 2023"):
      {"gte": "2023-01-01T00:00:00", "lte": "2023-01-05T23:59:59"}
  - Open-ended from a date (e.g. "from 1/1/2023" with no end stated):
      {"gte": "2023-01-01T00:00:00"}
  - Relative window (e.g. "last hour", "last 24h", "last week", "last 30 days"):
      {"gte": "now-1h"} / {"gte": "now-24h"} / {"gte": "now-7d"} / {"gte": "now-30d"}
  - Always use ISO-8601 for absolute dates.

## Output format
Respond with ONLY a valid JSON object. No markdown, no explanation, no extra text.

{
  "intent": "<retrieval|correlation|remediation|context_aware|off_topic>",
  "filters": {
    "level":       "<ERROR|WARN|INFO|DEBUG|CRITICAL|null>",
    "service":     "<service_name|null>",
    "keyword":     "<keyword|null>",
    "date_filter": {"gte": "<ISO-8601 or now-Xh/d/w>", "lte": "<ISO-8601>"} or null
  }
}
""")

QUERY_GEN_SYSTEM = SystemMessage(content="""
You are an OpenSearch DSL query builder for a SOC log analytics platform.

## Index: `otel-logs-validated`

Field reference:
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

## Query strategy by intent

retrieval:
  - Goal: fetch matching documents for human review
  - Use: bool/must with term/match/multi_match clauses
  - Sort: timestamp desc
  - Size: 50

correlation:
  - Goal: surface patterns, counts, distributions
  - Use: aggregations — terms agg on service.name, level, event.action, user.ip
  - Size: 0 (no raw hits needed)
  - Include: "aggs" block with meaningful bucket names

remediation:
  - Goal: pinpoint the specific events behind an incident
  - Use: bool/must targeting the exact error/service/IP mentioned
  - Sort: timestamp desc
  - Size: 20

## Rules
- Return ONLY the raw JSON body for the OpenSearch `body` parameter
- No markdown fences, no explanation, no wrapper keys outside the body
- Do NOT include any range or date clauses — there is no time filtering
- For keyword fields (service.name, user.ip, error.code) use `term`, not `match`
- For text fields (message, error.stack_trace) use `match` or `multi_match`
""")

OFF_TOPIC_SYSTEM = SystemMessage(content="""
You are a SOC assistant that specialises exclusively in infrastructure logs, security monitoring, and incident response.

The user has asked something outside your domain. Respond in 2-3 sentences:
1. Acknowledge you can't help with that topic.
2. Briefly redirect to what you CAN do — give 2 concrete example questions they could ask instead.

Be friendly but concise. No bullet lists, no headers — plain conversational text.
""")

ANALYST_SYSTEM = SystemMessage(content="""
You are a senior SOC analyst. Your job is to turn raw log data into a single, focused answer.

## Timestamp formatting
Always display timestamps in European format: DD/MM/YYYY HH:MM
Example: 2026-03-12T14:30:00Z → 12/03/2026 14:30
Convert every ISO-8601 timestamp in the log data before presenting it to the user.

## Output rules — strictly one section per intent

### If intent = RETRIEVAL
Present events as a markdown table: Timestamp | Level | Service | Message | User IP
- Keep Message to ≤ 80 chars (truncate with …)
- Sort newest first
- If 0 results: one sentence saying nothing matched, suggest broadening filters

### If intent = CONTEXT_AWARE
The user is asking a follow-up about data ALREADY shown in this conversation.
- Answer directly from the conversation history — NEVER say "no logs found"
- "how many?" → count the rows/entries from the previous answer and state the number clearly
- "what do they say?" → summarize the messages from the previously shown logs
- "tell me more / explain" → elaborate on the most recent log data in context
- "group / sort / reformat" → restructure the already-shown data accordingly
- Never suggest re-querying or broadening filters — the data is already in context
- Be concise and direct

### If intent = CORRELATION
Write a structured threat/pattern assessment.

LOG ANALYSIS PROCESS

Step 1 – Log Interpretation
Extract: timestamp, source IP, user/account, service/application, request path,
response code, error type, authentication result.

Step 2 – Event Correlation
Correlate: same source IP, same user, repeated errors, high request frequency,
unusual time windows, identical request patterns.

Step 3 – Threat & Pattern Detection
Identify: brute force, credential stuffing, port scanning, DDoS/traffic spikes,
repeated auth failures, privilege escalation, abnormal traffic, enumeration.
Prioritize patterns across multiple logs, not isolated events.

Step 4 – Attack Confirmation
If pattern detected: attack type, start time, source, target, IOCs, severity.
If evidence is weak: classify as Suspicious Activity, not confirmed attack.

OUTPUT — if NO suspicious activity:

STATUS: NO SIGNIFICANT THREAT DETECTED

ASSESSMENT: Brief explanation of what was analyzed.
RESULTS: 0 suspicious patterns identified.
SUGGESTION: Verify the correct log index or data source.

OUTPUT — if suspicious activity IS detected:

STATUS: POSSIBLE ATTACK OR SUSPICIOUS ACTIVITY DETECTED

PRIMARY FINDING: Most important detected pattern.

THREAT / PATTERN ASSESSMENT: Structured analysis grouped by most meaningful dimension.

IOC HIGHLIGHTS: List indicators of compromise.

TIMELINE: When activity started and key spikes (DD/MM/YYYY HH:MM).

AFFECTED TARGETS: Services, hosts, or users involved.

RELATED LOG EVENTS:
Markdown table: Timestamp | Level | Service | Message | User IP
- Keep Message ≤ 80 chars, sort newest first
- If > 50 logs match: summarize and show only the most representative 10

TECHNICAL DETAILS: Why the correlated behavior is suspicious.

RISK SUMMARY: One sentence — risk level and impact.

---
After presenting, ask: "Suspicious activity may be present. Would you like to see recommended mitigation and response actions?"
Only provide mitigation steps if the user explicitly asks.

### If intent = REMEDIATION
Generate clear numbered action steps based on detected attack patterns, logs, or IOCs.

For each step:
- Reference the exact IP, service, error code, user, or container from the logs
- State the specific command or configuration change needed
- Include the expected outcome

Step structure:

Step X — [Title]
Target: <entity from logs>
Action: <specific command or config change>
Expected Outcome: <what it mitigates or prevents>

If no logs or IOCs were identified:

NO REMEDIATION ACTION REQUIRED
Suggest running a correlation query first to identify affected systems.

## Global rules
- ONE section only — never output content for a different intent
- Always cite specifics from the log data (timestamps, IPs, codes, service names)
- Professional and direct — no filler phrases
- Use markdown but keep it focused
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


def _clean(raw: str) -> str:
    return raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()


# ── Nodes ─────────────────────────────────────────────────────────────────────

def intent_node(state: ChatState) -> ChatState:
    """LLM CALL 1 — Classify intent and extract filters."""
    history  = _history_messages(state.get("chat_history", []))
    messages = [INTENT_SYSTEM] + history + [HumanMessage(content=state["user_question"])]

    response = llm.invoke(messages)
    raw      = _clean(response.content)

    _print_llm(
        node="intent_node  (LLM CALL 1)",
        prompt_summary=f"Classify: \"{state['user_question']}\"",
        response=raw,
    )

    try:
        parsed  = json.loads(raw)
        intent  = parsed.get("intent", "retrieval")
        filters = parsed.get("filters", {})
    except Exception:
        logger.warning("[INTENT] Failed to parse JSON — defaulting to retrieval")
        intent  = "retrieval"
        filters = {}

    logger.info("[INTENT] → %s   filters=%s", intent, filters)
    return {**state, "intent": intent, "filters": filters}


def route_after_intent(state: ChatState) -> str:
    """Conditional edge: route based on intent."""
    intent = state["intent"]
    if intent == "off_topic":
        logger.info("[ROUTE] off_topic → off_topic_node")
        return "off_topic_node"
    if intent == "context_aware":
        logger.info("[ROUTE] context_aware → analyst_node (no OpenSearch)")
        return "analyst_node"
    logger.info("[ROUTE] %s → query_gen_node", intent)
    return "query_gen_node"


def off_topic_node(state: ChatState) -> ChatState:
    """LLM CALL 2a — Polite redirect for off-topic questions."""
    history  = _history_messages(state.get("chat_history", []))
    messages = [OFF_TOPIC_SYSTEM] + history + [HumanMessage(content=state["user_question"])]

    response = llm.invoke(messages)
    answer   = response.content

    _print_llm(
        node="off_topic_node  (LLM CALL 2a — SHORT CIRCUIT)",
        prompt_summary=f"Off-topic: \"{state['user_question']}\"",
        response=answer,
    )

    chat_history = list(state.get("chat_history", []))
    chat_history.append({"role": "user",      "content": state["user_question"]})
    chat_history.append({"role": "assistant", "content": answer})

    logger.info("[OFF_TOPIC] Done — %d chars", len(answer))
    return {**state, "answer": answer, "chat_history": chat_history}


def query_gen_node(state: ChatState) -> ChatState:
    """LLM CALL 2b — Generate OpenSearch DSL query from intent + filters."""
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
        logger.warning("[QUERY_GEN] Failed to parse DSL — using empty query")
        query = {}

    # ── Build a clean bool/must query ────────────────────────────────────────
    must = []

    # Salvage LLM-generated clauses (skip stray range clauses — we control dating)
    llm_query = query.get("query", {})
    for key in ("match", "term", "multi_match", "match_phrase"):
        if key in llm_query:
            must.append({key: llm_query[key]})
    if "bool" in llm_query:
        for clause in llm_query["bool"].get("must", []):
            if "range" not in clause:
                must.append(clause)

    # Hard-inject structured filters from intent classifier
    if filters.get("level"):
        must.append({"term": {"level": filters["level"].upper()}})
    if filters.get("service"):
        must.append({"match": {"service.name": filters["service"]}})
    if filters.get("keyword"):
        must.append({"multi_match": {
            "query":  filters["keyword"],
            "fields": [
                "message", "error.code", "user.ip", "user.id",
                "event.action", "metadata.region", "service.name", "event.category",
            ],
        }})

    # Inject date filter only when the user explicitly specified one
    date_filter = filters.get("date_filter")
    if date_filter and isinstance(date_filter, dict):
        must.append({"range": {"timestamp": date_filter}})

    # If no clauses at all, match everything
    final_query_clause = {"bool": {"must": must}} if must else {"match_all": {}}

    final_query = {
        "query": final_query_clause,
        "sort":  [{"timestamp": {"order": "desc"}}],
        "size":  query.get("size", 50),
    }
    if "aggs" in query:
        final_query["aggs"] = query["aggs"]
    if "aggregations" in query:
        final_query["aggregations"] = query["aggregations"]

    logger.info("[QUERY_GEN] Final DSL: %s", json.dumps(final_query)[:400])
    return {**state, "os_query": final_query}


def opensearch_node(state: ChatState) -> ChatState:
    """Execute the OpenSearch DSL query — NO LLM."""
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
    """LLM CALL 3 — Answer from OpenSearch results or chat history (context_aware)."""
    # os_results is None when context_aware bypassed OpenSearch — normalise to []
    results  = state.get("os_results") or []
    intent   = state["intent"]
    question = state["user_question"]
    history  = _history_messages(state.get("chat_history", []))

    if intent == "context_aware":
        # Answer purely from conversation history — no new data needed
        logger.info("[ANALYST] context_aware → using chat history only")
        context_msg = HumanMessage(content=(
            f"The user is asking a follow-up question about log data already shown "
            f"in this conversation.\n\n"
            f"User question: {question}\n\n"
            f"INTENT: CONTEXT_AWARE\n\n"
            f"Answer using only the data already present in the conversation above. "
            f"Do NOT say 'no logs found'. Do NOT suggest re-querying. "
            f"Convert any timestamps to DD/MM/YYYY HH:MM format."
        ))
        result_count = 0
    else:
        results_text = json.dumps(results[:50], indent=2) if results else "No logs found."
        result_count = len(results)
        context_msg = HumanMessage(content=(
            f"User question: {question}\n\n"
            f"INTENT: {intent.upper()}\n\n"
            f"Log data ({result_count} result(s)):\n"
            f"```json\n{results_text}\n```\n\n"
            f"Produce a {intent} answer. No other sections. "
            f"Convert all timestamps to DD/MM/YYYY HH:MM format."
        ))

    messages = [ANALYST_SYSTEM] + history + [context_msg]
    response = llm.invoke(messages)
    answer   = response.content

    _print_llm(
        node="analyst_node  (LLM CALL 3)",
        prompt_summary=f"intent={intent}  results={result_count}  q=\"{question}\"",
        response=answer,
    )

    chat_history = list(state.get("chat_history", []))
    chat_history.append({"role": "user",      "content": question})
    chat_history.append({"role": "assistant", "content": answer})

    logger.info("[ANALYST] Answer — %d chars", len(answer))
    return {**state, "answer": answer, "chat_history": chat_history}


# ── Graph builder ─────────────────────────────────────────────────────────────

def build_soc_graph():
    g = StateGraph(ChatState)

    g.add_node("intent_node",     intent_node)
    g.add_node("off_topic_node",  off_topic_node)
    g.add_node("query_gen_node",  query_gen_node)
    g.add_node("opensearch_node", opensearch_node)
    g.add_node("analyst_node",    analyst_node)

    g.add_edge(START, "intent_node")

    g.add_conditional_edges(
        "intent_node",
        route_after_intent,
        {
            "off_topic_node": "off_topic_node",
            "analyst_node":   "analyst_node",    # context_aware bypasses OpenSearch
            "query_gen_node": "query_gen_node",
        },
    )

    g.add_edge("off_topic_node",  END)
    g.add_edge("query_gen_node",  "opensearch_node")
    g.add_edge("opensearch_node", "analyst_node")
    g.add_edge("analyst_node",    END)

    return g.compile()


# ── Compiled pipeline (imported by app.py) ────────────────────────────────────
soc_pipeline = build_soc_graph()