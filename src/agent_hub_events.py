"""Agent Hub SSE event stream — push task updates to connected clients.

Per-owner subscriber sets so a client only receives their own tasks.
Uses ``asyncio.Queue`` for each connected client (pattern from src/agent_runs.py),
bounded to prevent slow-client memory leaks.

Event vocabulary:
  init              — full task list snapshot on connect
  task_created      — new task (full snapshot)
  task_updated      — task changed (full snapshot)
  task_deleted      — task removed (id only)
  event_created     — new timeline event (task snapshot)
  coordinator_status — coordinator running/idle
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Optional, Set

from fastapi import Request

from core.database import SessionLocal, AgentTask

logger = logging.getLogger(__name__)

# Per-owner subscriber sets: {owner: {asyncio.Queue, ...}}
_subscribers: Dict[str, Set[asyncio.Queue]] = {}

# Keepalive in seconds (SSE comment sent if no events for this long)
_KEEPALIVE_S = 30

# Max queued events per client before dropping
_QUEUE_MAXSIZE = 256


def _task_to_ssedict(t: AgentTask) -> dict:
    """Convert a task to a JSON-safe dict for SSE — MUST match the shape the
    frontend expects (same as _task_to_dict in agent_hub_routes, minus events
    for the list-level snapshots)."""
    started = None
    if t.locked_at:
        started = t.locked_at.isoformat() + "Z"
    elif t.events:
        for e in t.events:
            if getattr(e, "event_type", "") == "lock":
                started = e.created_at.isoformat() + "Z" if e.created_at else None
                break

    return {
        "id": t.id,
        "owner": t.owner,
        "title": t.title,
        "objective": t.objective,
        "status": t.status,
        "phase": t.phase,
        "current_owner": t.current_owner,
        "approval_required": t.approval_required,
        "locked_by": t.locked_by,
        "locked_at": t.locked_at.isoformat() + "Z" if t.locked_at else None,
        "attempt_count": t.attempt_count,
        "last_error": t.last_error,
        "session_id": t.session_id,
        "chain_task_id": t.chain_task_id,
        "sandbox_mode": t.sandbox_mode,
        "depends_on": t.depends_on,
        "created_by_task_id": t.created_by_task_id,
        "started_at": started,
        "created_at": t.created_at.isoformat() + "Z" if t.created_at else None,
        "updated_at": t.updated_at.isoformat() + "Z" if t.updated_at else None,
        "events": [_event_to_dict(e) for e in (t.events or [])],
    }


def _event_to_dict(e) -> dict:
    return {
        "id": e.id,
        "task_id": e.task_id,
        "actor": e.actor,
        "event_type": e.event_type,
        "summary": e.summary,
        "content": e.content,
        "metadata_json": e.metadata_json,
        "created_at": e.created_at.isoformat() + "Z" if e.created_at else None,
    }


def _make_sse(event: str, data: dict) -> str:
    """Format a single SSE event string."""
    payload = json.dumps(data, default=str)
    return f"event: {event}\ndata: {payload}\n\n"


def publish(owner: str, event: str, data: dict) -> None:
    """Publish an Agent Hub event to all SSE clients of the given owner.

    Args:
        owner: The task owner (username). Events are only sent to this owner's
               connected clients. Pass empty string for ownerless tasks.
        event: One of the event vocabulary (init, task_created, task_updated,
               task_deleted, event_created, coordinator_status).
        data: JSON-serialisable dict with event payload.
    """
    if not owner:
        return
    queues = _subscribers.get(owner)
    if not queues:
        return
    sse_str = _make_sse(event, data)
    dead: set = set()
    for q in queues:
        try:
            q.put_nowait(sse_str)
        except asyncio.QueueFull:
            dead.add(q)
    if dead:
        queues -= dead
        logger.debug(
            "agent_hub_events: dropped %d slow subscriber(s) for %s",
            len(dead), owner,
        )


def _get_all_tasks(owner: str) -> list[dict]:
    """Fetch all tasks for an owner (used for init event)."""
    db = SessionLocal()
    try:
        tasks = (
            db.query(AgentTask)
            .filter(AgentTask.owner == owner)
            .order_by(AgentTask.updated_at.desc())
            .all()
        )
        return [_task_to_ssedict(t) for t in tasks]
    finally:
        db.close()


def _resolve_event_owner(owner_from_auth: Optional[str]) -> str:
    """Return the authenticated owner, or empty string if none."""
    return (owner_from_auth or "").strip()


async def subscribe(owner: str, request: Request) -> AsyncGenerator[str, None]:
    """SSE async generator — send init snapshot then live events.

    Args:
        owner: The authenticated user. Only receives events for their tasks.
        request: FastAPI request for disconnect detection.

    Yields:
        SSE-formatted strings (``event: ...\\ndata: ...\\n\\n``).
    """
    if not owner:
        # No owner = no tasks to stream. Yield an empty init and exit.
        yield _make_sse("init", {"tasks": []})
        return

    q: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
    queues = _subscribers.setdefault(owner, set())
    queues.add(q)

    try:
        # ── Init: full task list ──
        tasks = _get_all_tasks(owner)
        yield _make_sse("init", {"tasks": tasks})

        # ── Live events ──
        while True:
            if await request.is_disconnected():
                break
            try:
                sse_str = await asyncio.wait_for(q.get(), timeout=_KEEPALIVE_S)
                yield sse_str
            except asyncio.TimeoutError:
                # SSE comment as keepalive (browsers ignore lines starting with ':')
                yield ": keepalive\n\n"
    finally:
        queues.discard(q)
        if not queues:
            _subscribers.pop(owner, None)


def subscriber_count(owner: str) -> int:
    """Return number of connected SSE clients for an owner (useful for debug)."""
    qs = _subscribers.get(owner)
    return len(qs) if qs else 0
