"""
SOC Assistant — FastAPI server
================================
POST /chat          — send a message, get an answer
GET  /history/{id}  — get conversation history for a session
DELETE /history/{id}— clear session history
GET  /health        — health check
"""

import logging
import uuid
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from soc_assistant import ChatState, soc_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("app")

app = FastAPI(title="SOC Assistant", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── In-memory session store (replace with Redis for production) ────────────────
sessions: dict[str, list[dict]] = {}


# ── Request / Response models ──────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question:   str
    session_id: Optional[str] = None   # omit to start a new session


class ChatResponse(BaseModel):
    session_id: str
    answer:     str
    intent:     str
    results_count: int


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    session_id   = req.session_id or str(uuid.uuid4())
    chat_history = sessions.get(session_id, [])

    initial_state: ChatState = {
        "session_id":    session_id,
        "chat_history":  chat_history,
        "user_question": req.question,
        "intent":        None,
        "filters":       None,
        "os_query":      None,
        "os_results":    None,
        "answer":        None,
    }

    try:
        final_state = soc_pipeline.invoke(initial_state)
    except Exception as exc:
        logger.error("[CHAT] Pipeline error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))

    # Persist updated history
    sessions[session_id] = final_state["chat_history"]

    return ChatResponse(
        session_id    = session_id,
        answer        = final_state["answer"],
        intent        = final_state["intent"],
        results_count = len(final_state.get("os_results") or []),
    )


@app.get("/history/{session_id}")
def get_history(session_id: str):
    history = sessions.get(session_id)
    if history is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"session_id": session_id, "history": history}


@app.delete("/history/{session_id}")
def clear_history(session_id: str):
    sessions.pop(session_id, None)
    return {"status": "cleared", "session_id": session_id}


# ── Serve the web UI ───────────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def serve_ui():
    return FileResponse("static/index.html")