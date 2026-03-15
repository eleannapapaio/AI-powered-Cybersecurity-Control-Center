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
    http_auth=("admin", os.environ["OPENSEARCH_PASSWORD"]),
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

## CRITICAL DISAMBIGUATION — counting questions

Questions asking for counts (e.g. "how many logs", "how many errors") are **NOT automatically context_aware**.

Use `retrieval` when:
- The user asks about logs in general
- The user asks how many logs exist
- The user asks how many errors / warnings / events occurred
- The user does NOT explicitly reference previously shown results

Examples → retrieval:
"how many logs are there"
"how many error logs exist"
"count all logs"
"how many logs do you see"
"how many events happened"

Use `context_aware` ONLY if the user clearly refers to logs that were already shown.

Examples → context_aware:
"how many of those logs are errors"
"how many of the logs you showed are from Germany"
"how many are WARN"
"how many entries are there in that table"

## HARD RULE

If the conversation has NOT yet shown any log data from OpenSearch,
DO NOT classify the question as `context_aware`.

In that case you MUST choose:
retrieval, correlation, or remediation.

Use a fresh retrieval/correlation/remediation (NOT context_aware) when:
- The user introduces a new filter, keyword, service, IP, or region
- The user asks about something not yet fetched (e.g. "now show me WARN logs" after seeing ERROR logs)
- There is no prior assistant answer in the conversation

## Filter extraction rules
For retrieval / correlation / remediation ONLY (not for context_aware or off_topic):

- level: One of ERROR, WARN, INFO, DEBUG, CRITICAL — null if not specified
- service: Exact service name if mentioned — null otherwise, never invent service names. Only extract service names that appear explicitly in the user message.
- keyword: A SPECIFIC TECHNICAL TERM to search for — IP address, user ID, error code,
  HTTP method, country, region, action, browser, OS, or any notable word from the message.
  Prefer the most specific term the user mentions. null if none.
  CRITICAL: Do NOT extract the user's question intent as a keyword.
  "suspicious activity", "anomalies", "threats", "attacks", "errors", "logs" are NOT keywords.
  Only extract a keyword when the user names a specific entity: an IP, a username, a port,
  a service name, an error code, or a specific message fragment.
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

## Uncertainty rule
If the user request is ambiguous or unclear,
choose the MOST LIKELY intent based on the wording.
Do not invent filters.
If no filter is explicitly stated, return null values.
                              
""")

QUERY_GEN_SYSTEM = SystemMessage(content="""
You are an OpenSearch DSL query builder for a SOC log analytics platform that detects suspicious activity and potential cyber attacks.
                                 
Your job is to convert a user's security investigation request into a valid OpenSearch DSL query body.

You must generate queries that help SOC analysts investigate suspicious behavior in infrastructure logs.

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

## Security detection goals

The queries should help identify:

1. Brute force login attempts

   * many repeated actions from the same user.ip
   * repeated authentication failures in message or error.code

2. Error spikes that indicate system failure or attack

   * large numbers of ERROR or WARN level logs
   * repeated exception or failure messages

3. Suspicious IP behaviour

   * one IP triggering many events
   * one IP interacting with multiple services

4. Service anomalies

   * abnormal concentration of errors in a specific service.name

5. Potential exploitation attempts
                                 
   * messages containing "suspicious", "attack", "unauthorized", "denied", "failed", "exception"

## DO NOT USE GENERIC SEARCH TERMS
Never build queries using generic words such as:

logs
events
activity
issues
problems
things

These are not searchable indicators.

## Query strategy by intent
The user intent will be provided in the prompt.

INTENT = retrieval:
Goal: fetch suspicious events for human investigation

Query rules:

• Use bool/must
• Use term for keyword fields
• Use match or multi_match for text fields
• Sort by timestamp descending
• Size = 50

Suggested message keywords:

* "failed"
* "unauthorized"
* "denied"
* "exception"
* "attack"
                                 
If a keyword filter exists, search in:

message
error.code
user.ip
user.id
event.action
metadata.region
service.name
event.category                                 

INTENT = correlation:
Goal: detect patterns and anomalies

Use:

* size: 100  (raw hits are REQUIRED — analyst reads message text for port scans)
* aggregations alongside hits
* return raw logs AND aggregations
* size MUST be 100 (never 0)

Required aggregations:

* terms on user.ip.keyword
* terms on service.name.keyword
* terms on level.keyword
* terms on event.action.keyword

Purpose:

* identify IPs generating many events
* detect services with high error rates
* identify repeated actions or patterns
* detect port scans from ACL-denied messages targeting different destination ports
* support detection of scanning or probing patterns


remediation:
Goal: pinpoint the events responsible for an incident

Use:

* Use precise filters for IP, service, error code or action
* Sort by timestamp descending
* Size = 20

## DATE FILTERING
* Do NOT add time filters unless the user explicitly requested one.
* If a date_filter exists in the filters input, apply it to the timestamp field.

## EMPTY FILTER RULE

If the user provided no filters (no keyword, service, level, or IP), the query  MUST be:

{
"query": { "match_all": {} }
}

Do NOT invent filters or search terms.

                                 
Field usage rules:
Always prefer filtering using structured fields (user.ip, user.id, service.name, error.code) before searching inside the message field.

keyword fields:

* service.name
* user.ip
* error.code
* user.id
* event.action
* metadata.region
  → use `term`

text fields:

* message
* error.stack_trace
  → use `match` or `multi_match`

Aggregation usage

When using aggregations with `terms`, ALWAYS use the `.keyword` version
of fields if they are text fields.

Examples:

user.ip.keyword
service.name.keyword
event.action.keyword
level.keyword

Never aggregate on raw text fields like:
user.ip
service.name
message
error.stack_trace
                                 
Example 1 - retrieval 
User request: "show ERROR logs from the payment service"
Query:
{
  "query": {
    "bool": {
      "must": [
        { "term": { "level": "ERROR" }},
        { "term": { "service.name": "payment-service" }}
      ]
    }
  },
  "sort": [{ "timestamp": { "order": "desc" }}],
  "size": 50
}
                                 
Example 2 - retrieval                                 
User request: "show logs from IP 192.168.1.10"
Query:                                 
{
  "query": {
    "bool": {
      "must": [
        { "term": { "user.ip": "192.168.1.10" }}
      ]
    }
  },
  "sort": [{ "timestamp": { "order": "desc" }}],
  "size": 50
}
                                 
Example 3 - correlation
User request: "find suspicious activity from IP 10.0.0.5" 
Query:
{
  "query": {
    "bool": {
      "must": [
        { "term": { "user.ip": "10.0.0.5" }}
      ]
    }
  },
  "sort": [{ "timestamp": { "order": "desc" }}],
  "size": 100,
  "aggs": {
    "top_ips": {
      "terms": { "field": "user.ip.keyword", "size": 10 }
    },
    "top_services": {
      "terms": { "field": "service.name.keyword", "size": 10 }
    },
    "top_levels": {
      "terms": { "field": "level.keyword", "size": 10 }
    },
    "top_actions": {
      "terms": { "field": "event.action.keyword", "size": 10 }
    }
  }
}
                                 
Example 4 - remediation
User request: "investigate authentication failures for user john"
Query:
{
  "query": {
    "bool": {
      "must": [
        { "term": { "user.id": "john" }},
        { "match": { "message": "authentication failed" }}
      ]
    }
  },
  "sort": [{ "timestamp": { "order": "desc" }}],
  "size": 20
}
                                 
Examples demonstrate the structure of correct queries.
Do NOT copy them directly. Adapt them to the user request.

## OUTPUT FORMAT

* Return ONLY valid JSON body for the OpenSearch `body` parameter. The first character must be '{' and the last character must be '}'.
* Do NOT include markdown fences
* Do NOT include explanations
* Do NOT include wrapper keys outside the body
* Do NOT include time filtering unless a date_filter is provided in the filters input.

Example correct aggregation:

"aggs": {
  "top_ips": {
    "terms": {
      "field": "user.ip.keyword",
      "size": 10
    }
  }
}
                                 
## HALLUCINATION RULE

Never invent fields, filters, or search terms that were not mentioned
in the user request or provided filters.

If uncertain, return a broad query rather than a restrictive one.
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

## Log data format
The log data array always begins with a metadata object {"_total_hits": N} — the true
total count of matching documents. Use this to answer "how many" questions accurately.
The remaining elements are the actual log records (up to 100 for correlation, 50 for retrieval).

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
If message contains "access denied by ACL," extract the source IP and the destination port (the number after the / at the end of the message).
When analyzing a threat, you MUST query for at least the last 100 logs and look for a sequence of events. Do not rely on a single log entry to make a determination.

Step 2 – Event Correlation
Correlate: same source IP, same user, repeated errors, high request frequency,
unusual time windows, identical request patterns.
STRICT RULE: You must parse the message string. 
If you see 'TCP access denied' multiple times from the same user.ip, look at the port number at the very end of the message (e.g., /23, /80, /443). 
If these numbers are different, you MUST report a 'POSSIBLE PORT SCAN'.

Step 3 – Threat & Pattern Detection
Look explicitly for the following indicators in the logs:

Network indicators
- repeated TCP connections from the same source
- connections targeting sensitive ports (22, 3389, 445, 21, 23)
- connections marked as "suspicious" in the log message
- connections between internal hosts and EXT_SERVER
- unusually long or repeated connection durations

Authentication indicators
- repeated login failures
- authentication errors
- "unauthorized", "denied", "failed" in messages

Error indicators
- bursts of ERROR or WARN logs
- repeated exception or failure messages

Traffic anomalies
- one IP generating many events
- one service receiving unusually high traffic
- repeated identical actions

If any of these indicators appear, treat them as **suspicious activity** and explain why.
                               
SPECIAL RULE — Suspicious classification

If a log message contains the phrase "class suspicious",
you MUST treat this as a potential security indicator.

Explain:
- which IP initiated the connection
- which target host or port was contacted
- why this could indicate probing, scanning, or unauthorized access.
                               

Step 4 – Attack Confirmation
If pattern detected:
Clearly state:
Attack Type (or Suspicious Activity Type)
Source IP(s)
Target host or service
Target port (if visible)
Time range of activity
Why the behavior is suspicious

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

    # Only salvage LLM-generated clauses when the user actually specified
    # a keyword/service/level filter — otherwise the LLM injects the user's
    # question text as a search keyword (e.g. "suspicious activity") which
    # silently filters out all unrelated logs.
    has_structured_filters = any([
        filters.get("level"),
        filters.get("service"),
        filters.get("keyword"),
    ])

    if has_structured_filters:
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
        must.append({"term": {"level": filters["level"].lower()}})
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

    # For correlation always fetch raw hits so the analyst can read message
    # text for port-scan detection, ACL patterns, etc.
    if state["intent"] == "correlation":
        size = 100
    else:
        size = query.get("size", 50)

    final_query = {
        "query": final_query_clause,
        "sort": [{"timestamp": {"order": "desc"}}],
        "size": size,
        "track_total_hits": True,
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
        resp       = os_client.search(index=INDEX_NAME, body=query)
        hits       = [h["_source"] for h in resp["hits"]["hits"]]
        aggs       = resp.get("aggregations", {})
        total_hits = resp["hits"]["total"]["value"] if isinstance(resp["hits"]["total"], dict)                      else resp["hits"]["total"]
        results    = [{"_total_hits": total_hits}] + hits + ([{"_aggregations": aggs}] if aggs else [])
        logger.info("[OPENSEARCH] %d hit(s) returned  (total matching: %d)", len(hits), total_hits)
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