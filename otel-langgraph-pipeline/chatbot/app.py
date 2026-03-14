"""
================================================================================
SOC ASSISTANT — FASTAPI SERVER
================================================================================
API Endpoints:
  - POST   /chat             -> Send a message and receive an AI-generated answer
  - GET    /history/{id}     -> Retrieve conversation history for a session
  - DELETE /history/{id}     -> Clear specific session history
  - GET    /health           -> Service health check
================================================================================
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

# --- Logging Configuration ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("app")

# --- App Initialization -------------------------------------------------------
app = FastAPI(title="SOC Assistant", version="1.0.0")

# --- Middleware ---------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Data Storage -------------------------------------------------------------
# In-memory session store (Note: Replace with Redis for production scalability)
sessions: dict[str, list[dict]] = {}


# --- Request / Response Schemas -----------------------------------------------

class ChatRequest(BaseModel):
    question:   str
    session_id: Optional[str] = None   # Leave empty to generate a new session


class ChatResponse(BaseModel):
    session_id: str
    answer:     str
    intent:     str
    results_count: int


# --- API Routes ---------------------------------------------------------------

@app.get("/health")
def health():
    """Verify the server is up and running."""
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    """
    Main chat endpoint. Processes user questions through the SOC pipeline 
    and maintains session state.
    """
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
        # Execute the SOC pipeline logic
        final_state = soc_pipeline.invoke(initial_state)
    except Exception as exc:
        logger.error("[CHAT] Pipeline error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))

    # Persist updated history back to the session store
    sessions[session_id] = final_state["chat_history"]

    return ChatResponse(
        session_id    = session_id,
        answer        = final_state["answer"],
        intent        = final_state["intent"],
        results_count = len(final_state.get("os_results") or []),
    )


@app.get("/history/{session_id}")
def get_history(session_id: str):
    """Fetch the chat history for a specific session."""
    history = sessions.get(session_id)
    if history is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"session_id": session_id, "history": history}


@app.delete("/history/{session_id}")
def clear_history(session_id: str):
    """Remove a session and its history from the store."""
    sessions.pop(session_id, None)
    return {"status": "cleared", "session_id": session_id}


# --- Web UI Serving -----------------------------------------------------------

# Mount the directory for CSS/JS/Images
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def serve_ui():
    """Serve the main Web UI landing page."""
    return FileResponse("static/index.html")