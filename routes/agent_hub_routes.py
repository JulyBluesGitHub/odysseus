"""Agent Hub routes — task CRUD, events, assign, approve, transition, and coordinator status.

The Agent Hub is a multi-agent cockpit where the user creates tasks, assigns them
to agents (user / hermes / codex / cursor), and watches a real-time timeline of
events. This module provides the REST API backing that UI.

Route convention: ``setup_agent_hub_routes() -> APIRouter`` (matches the existing
``setup_*_routes()`` pattern used everywhere in `app.py`).
"""

import json
import logging
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

from core.database import SessionLocal, AgentTask, AgentEvent
from sqlalchemy import or_
from src.auth_helpers import get_current_user

logger = logging.getLogger(__name__)


# ── Pydantic schemas ──────────────────────────────────────────────────────────

VALID_STATUSES = {
    "draft", "queued", "running", "waiting_for_approval",
    "blocked", "done", "cancelled",
}

# Allowed transitions (source → set of valid targets).
# Any status can transition to "cancelled" (explicit cancel path).
TRANSITIONS: dict[str, set[str]] = {
    "draft":                 {"queued", "cancelled"},
    "queued":                {"running", "cancelled"},
    "running":               {"waiting_for_approval", "queued", "blocked", "done", "cancelled"},
    "waiting_for_approval":  {"queued", "blocked", "done", "cancelled"},
    "blocked":               {"queued", "cancelled"},
    "done":                  set(),          # terminal
    "cancelled":             set(),          # terminal
}

VALID_ACTORS = {"user", "hermes", "codex", "cursor", "coordinator"}
VALID_EVENT_TYPES = {"message", "status_change", "approval", "error", "lock"}
VALID_OWNERS = {"user", "hermes", "codex", "cursor"}
VALID_SANDBOX_MODES = {"read-only", "workspace-write", "danger-full-access"}


class TaskCreate(BaseModel):
    title: str = "Untitled Task"
    objective: Optional[str] = None
    status: str = "draft"
    phase: Optional[str] = None
    current_owner: Optional[str] = None
    approval_required: bool = False
    session_id: Optional[str] = None
    chain_task_id: Optional[str] = None
    sandbox_mode: str = "workspace-write"


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    objective: Optional[str] = None
    status: Optional[str] = None
    phase: Optional[str] = None
    current_owner: Optional[str] = None
    approval_required: Optional[bool] = None
    session_id: Optional[str] = None
    chain_task_id: Optional[str] = None
    sandbox_mode: Optional[str] = None


class EventCreate(BaseModel):
    actor: str = "user"
    event_type: str = "message"
    summary: Optional[str] = None
    content: Optional[str] = None
    metadata_json: Optional[str] = None


class AssignRequest(BaseModel):
    current_owner: str  # user | hermes | codex | cursor


class TransitionRequest(BaseModel):
    status: str
    force_cancel: bool = False  # release lock when cancelling a running task


# ── Helpers ───────────────────────────────────────────────────────────────────

def _task_to_dict(t: AgentTask) -> dict:
    # Compute started_at: prefer locked_at, fall back to first lock event
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
        "started_at": started,
        "created_at": t.created_at.isoformat() + "Z" if t.created_at else None,
        "updated_at": t.updated_at.isoformat() + "Z" if t.updated_at else None,
        "events": [_event_to_dict(e) for e in (t.events or [])],
    }


def _event_to_dict(e: AgentEvent) -> dict:
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


def _validate_transition(current_status: str, new_status: str, locked_by: str | None,
                          force_cancel: bool = False) -> None:
    """Raise HTTPException if the transition is invalid."""
    if new_status not in VALID_STATUSES:
        raise HTTPException(400, f"Invalid status: {new_status}")

    allowed = TRANSITIONS.get(current_status, set())
    if new_status not in allowed:
        # Special case: force_cancel releases a lock on a running task
        if new_status == "cancelled" and force_cancel:
            return
        raise HTTPException(
            400,
            f"Cannot transition from '{current_status}' to '{new_status}'. "
            f"Allowed: {sorted(allowed)}"
        )

    # Guard: can't cancel a running task that's locked without force_cancel
    if new_status == "cancelled" and current_status == "running" and locked_by and not force_cancel:
        raise HTTPException(
            409,
            f"Task is locked by '{locked_by}'. Use force_cancel=true to override."
        )


# ── Router ────────────────────────────────────────────────────────────────────

def setup_agent_hub_routes() -> APIRouter:
    router = APIRouter(prefix="/api/agent-hub", tags=["agent-hub"])

    # ── SSE stream ──────────────────────────────────────────────────────────

    @router.get("/stream")
    async def agent_hub_stream(request: Request):
        """SSE event stream for Agent Hub — push task updates to the UI.

        The client receives an ``init`` event with all its tasks, then live
        ``task_created``, ``task_updated``, ``task_deleted``, ``event_created``,
        and ``coordinator_status`` events as they happen.  Only the
        authenticated user's own tasks are sent.
        """
        from src.agent_hub_events import subscribe as _ev_subscribe
        from src.auth_helpers import get_current_user

        user = get_current_user(request)
        return StreamingResponse(
            _ev_subscribe(user or "", request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # ── Task CRUD ─────────────────────────────────────────────────────────

    @router.get("/tasks")
    async def list_tasks(
        request: Request,
        status: Optional[str] = Query(None),
        owner_agent: Optional[str] = Query(None, alias="owner"),
        q: Optional[str] = Query(None, description="Free-text search on title and objective"),
    ):
        """List Agent Hub tasks, optionally filtered by status, owner, and/or keyword search."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            qobj = db.query(AgentTask)
            if user:
                qobj = qobj.filter(AgentTask.owner == user)
            if status:
                if status not in VALID_STATUSES:
                    raise HTTPException(400, f"Invalid status: {status}")
                qobj = qobj.filter(AgentTask.status == status)
            if owner_agent:
                qobj = qobj.filter(AgentTask.current_owner == owner_agent)
            if q:
                pattern = f"%{q}%"
                qobj = qobj.filter(or_(
                    AgentTask.title.ilike(pattern),
                    AgentTask.objective.ilike(pattern),
                ))
            tasks = qobj.order_by(AgentTask.updated_at.desc()).all()
            return {"tasks": [_task_to_dict(t) for t in tasks]}
        finally:
            db.close()

    @router.post("/tasks", status_code=201)
    async def create_task(request: Request, body: TaskCreate):
        """Create a new Agent Hub task."""
        user = get_current_user(request)
        if body.status not in VALID_STATUSES:
            raise HTTPException(400, f"Invalid status: {body.status}")
        if body.current_owner and body.current_owner not in VALID_OWNERS:
            raise HTTPException(400, f"Invalid current_owner: {body.current_owner}")
        if body.sandbox_mode not in VALID_SANDBOX_MODES:
            raise HTTPException(400, f"Invalid sandbox_mode: {body.sandbox_mode}")

        task = AgentTask(
            id=str(uuid.uuid4()),
            owner=user,
            title=body.title,
            objective=body.objective,
            status=body.status,
            phase=body.phase,
            current_owner=body.current_owner,
            approval_required=body.approval_required,
            session_id=body.session_id,
            chain_task_id=body.chain_task_id,
            sandbox_mode=body.sandbox_mode,
        )
        db = SessionLocal()
        try:
            db.add(task)
            db.commit()
            db.refresh(task)
            result = _task_to_dict(task)
            # Publish after commit succeeds
            from src.agent_hub_events import publish
            publish(user or "", "task_created", result)
            return result
        finally:
            db.close()

    @router.get("/tasks/{task_id}")
    async def get_task(request: Request, task_id: str):
        """Get a single task with its event timeline."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
            if not task:
                raise HTTPException(404, "Task not found")
            if user and task.owner and task.owner != user:
                raise HTTPException(404, "Task not found")
            return _task_to_dict(task)
        finally:
            db.close()

    @router.put("/tasks/{task_id}")
    async def update_task(request: Request, task_id: str, body: TaskUpdate):
        """Update task fields. Only non-None fields are applied."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
            if not task:
                raise HTTPException(404, "Task not found")
            if user and task.owner and task.owner != user:
                raise HTTPException(404, "Task not found")

            if body.status is not None:
                if body.status not in VALID_STATUSES:
                    raise HTTPException(400, f"Invalid status: {body.status}")
                _validate_transition(task.status, body.status, task.locked_by)
                # Record a status-change event for the transition
                old_status = task.status
                task.status = body.status
                _record_event(db, task_id, "coordinator", "status_change",
                              summary=f"Status: {old_status} → {body.status}")

            if body.title is not None:
                task.title = body.title
            if body.objective is not None:
                task.objective = body.objective
            if body.phase is not None:
                task.phase = body.phase
            if body.current_owner is not None:
                if body.current_owner not in VALID_OWNERS and body.current_owner != "":
                    raise HTTPException(400, f"Invalid current_owner: {body.current_owner}")
                task.current_owner = body.current_owner if body.current_owner else None
            if body.approval_required is not None:
                task.approval_required = body.approval_required
            if body.session_id is not None:
                task.session_id = body.session_id
            if body.chain_task_id is not None:
                task.chain_task_id = body.chain_task_id if body.chain_task_id else None
            if body.sandbox_mode is not None:
                if body.sandbox_mode not in VALID_SANDBOX_MODES:
                    raise HTTPException(400, f"Invalid sandbox_mode: {body.sandbox_mode}")
                task.sandbox_mode = body.sandbox_mode

            db.commit()
            db.refresh(task)
            result = _task_to_dict(task)
            from src.agent_hub_events import publish
            publish(user or "", "task_updated", result)
            return result
        finally:
            db.close()

    @router.delete("/tasks/{task_id}")
    async def delete_task(request: Request, task_id: str):
        """Delete a task and all its events (cascade)."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
            if not task:
                raise HTTPException(404, "Task not found")
            if user and task.owner and task.owner != user:
                raise HTTPException(404, "Task not found")
            db.delete(task)
            db.commit()
            from src.agent_hub_events import publish
            publish(user or "", "task_deleted", {"id": task_id})
            return {"ok": True}
        finally:
            db.close()

    # ── Events ────────────────────────────────────────────────────────────

    @router.post("/tasks/{task_id}/events", status_code=201)
    async def add_event(request: Request, task_id: str, body: EventCreate):
        """Add an event to a task's timeline."""
        user = get_current_user(request)
        if body.actor not in VALID_ACTORS:
            raise HTTPException(400, f"Invalid actor: {body.actor}")
        if body.event_type not in VALID_EVENT_TYPES:
            raise HTTPException(400, f"Invalid event_type: {body.event_type}")

        db = SessionLocal()
        try:
            task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
            if not task:
                raise HTTPException(404, "Task not found")
            if user and task.owner and task.owner != user:
                raise HTTPException(404, "Task not found")

            event = _record_event(
                db, task_id, body.actor, body.event_type,
                summary=body.summary, content=body.content,
                metadata_json=body.metadata_json,
            )
            db.commit()
            db.refresh(event)
            db.refresh(task)
            # Publish event_created with full task snapshot for timeline update
            from src.agent_hub_events import publish
            publish(user or "", "event_created", _task_to_dict(task))
            return _event_to_dict(event)
        finally:
            db.close()

    # ── Actions ───────────────────────────────────────────────────────────

    @router.post("/tasks/{task_id}/assign")
    async def assign_task(request: Request, task_id: str, body: AssignRequest):
        """Assign a task to an agent (user | hermes | codex | cursor)."""
        user = get_current_user(request)
        if body.current_owner not in VALID_OWNERS:
            raise HTTPException(400, f"Invalid current_owner: {body.current_owner}")

        db = SessionLocal()
        try:
            task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
            if not task:
                raise HTTPException(404, "Task not found")
            if user and task.owner and task.owner != user:
                raise HTTPException(404, "Task not found")

            old_owner = task.current_owner
            task.current_owner = body.current_owner
            # Auto-transition draft → queued when assigned to a non-user agent
            if task.status == "draft" and body.current_owner != "user":
                task.status = "queued"

            _record_event(db, task_id, "user", "status_change",
                          summary=f"Assigned to {body.current_owner}" +
                                  (f" (was {old_owner})" if old_owner and old_owner != body.current_owner else ""))
            db.commit()
            db.refresh(task)
            result = _task_to_dict(task)
            from src.agent_hub_events import publish
            publish(user or "", "task_updated", result)
            return result
        finally:
            db.close()

    @router.post("/tasks/{task_id}/approve")
    async def approve_task(request: Request, task_id: str):
        """User approves a task waiting for approval. Executes any pending
        actions proposed by the adapter, records results, and re-queues."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
            if not task:
                raise HTTPException(404, "Task not found")
            if user and task.owner and task.owner != user:
                raise HTTPException(404, "Task not found")

            if task.status != "waiting_for_approval":
                raise HTTPException(400, f"Task is '{task.status}', not 'waiting_for_approval'")

            # Go directly to done — actions execute next, coordinator shouldn't
            # pick this up mid-flight
            task.status = "done"
            task.approval_required = False
            task.locked_by = None
            # keep locked_at for timing
            _record_event(db, task_id, "user", "approval",
                          summary="User approved — executing pending actions")
            db.commit()

            # Fire chain immediately — task is done, activate the next one
            if task.chain_task_id:
                from src.agent_coordinator import _activate_chain
                _activate_chain(db, task)

            db.refresh(task)
            # Publish task update after approval + chain activation
            from src.agent_hub_events import publish
            publish(user or "", "task_updated", _task_to_dict(task))
        finally:
            db.close()

        # Execute pending actions and record results
        import asyncio as _asyncio
        from src.agent_coordinator import execute_pending_actions
        results = await _asyncio.to_thread(execute_pending_actions, task_id)

        # Re-fetch to get updated events
        db2 = SessionLocal()
        try:
            task = db2.query(AgentTask).filter(AgentTask.id == task_id).first()
            return {
                "task": _task_to_dict(task) if task else None,
                "actions_executed": len(results),
                "action_results": results,
            }
        finally:
            db2.close()

    @router.post("/tasks/{task_id}/transition")
    async def transition_task(request: Request, task_id: str, body: TransitionRequest):
        """Force a status transition, with optional force_cancel to release a lock."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
            if not task:
                raise HTTPException(404, "Task not found")
            if user and task.owner and task.owner != user:
                raise HTTPException(404, "Task not found")

            _validate_transition(task.status, body.status, task.locked_by,
                                  force_cancel=body.force_cancel)

            old_status = task.status
            task.status = body.status

            # If force-cancelling a locked task, release the lock
            if body.force_cancel and body.status == "cancelled":
                was_locked_by = task.locked_by
                task.locked_by = None
                _record_event(db, task_id, "user", "lock",
                              summary=f"Force-cancelled — lock released from {was_locked_by}")
            else:
                _record_event(db, task_id, "coordinator", "status_change",
                              summary=f"Status: {old_status} → {body.status}")

            db.commit()
            db.refresh(task)
            result = _task_to_dict(task)
            from src.agent_hub_events import publish
            publish(user or "", "task_updated", result)
            return result
        finally:
            db.close()

    @router.get("/tasks/{task_id}/export")
    async def export_task_timeline(request: Request, task_id: str):
        """Export a task's full timeline as a Markdown file."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
            if not task:
                raise HTTPException(404, "Task not found")
            if user and task.owner and task.owner != user:
                raise HTTPException(404, "Task not found")

            events = (db.query(AgentEvent)
                      .filter(AgentEvent.task_id == task_id)
                      .order_by(AgentEvent.created_at.asc())
                      .all())

            lines = []
            lines.append(f"# {task.title}")
            lines.append("")
            lines.append(f"**Status:** {task.status} | **Owner:** {task.current_owner or 'unassigned'} | **ID:** `{task.id}`")
            if task.phase:
                lines.append(f"**Phase:** {task.phase}")
            if task.chain_task_id:
                lines.append(f"**Chain:** triggers → `{task.chain_task_id}`")
            if task.objective:
                lines.append("")
                lines.append("## Objective")
                lines.append("")
                lines.append(task.objective)
            lines.append("")
            lines.append("## Timeline")
            lines.append("")

            if not events:
                lines.append("_No events recorded._")
            else:
                for e in events:
                    ts = e.created_at.strftime("%Y-%m-%d %H:%M:%S") if e.created_at else "unknown"
                    lines.append(f"### [{ts}] {e.actor} — {e.event_type}")
                    if e.summary:
                        lines.append("")
                        lines.append(e.summary)
                    if e.content:
                        lines.append("")
                        lines.append("```")
                        lines.append(e.content)
                        lines.append("```")
                    if e.metadata_json:
                        try:
                            meta = json.loads(e.metadata_json)
                            if meta.get("actions"):
                                lines.append("")
                                lines.append("**Actions:**")
                                for a in meta["actions"]:
                                    lines.append(f"- `{a.get('type', '?')}`: {a.get('label', '')}")
                            if meta.get("actions_pending"):
                                lines.append("_(Pending approval)_")
                        except Exception:
                            pass
                    lines.append("")

            lines.append("---")
            lines.append(f"_Exported {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}_")

            md = "\n".join(lines)
            return Response(
                content=md,
                media_type="text/markdown",
                headers={"Content-Disposition": f"attachment; filename=task-{task_id[:8]}.md"},
            )
        finally:
            db.close()

    # ── Coordinator status ────────────────────────────────────────────────

    @router.get("/status")
    async def coordinator_status(request: Request):
        """Return the coordinator's live status (running, last tick, adapters)."""
        try:
            from src.agent_coordinator import get_status as _coord_status
            return _coord_status()
        except Exception:
            return {
                "running": False,
                "last_tick": None,
                "tasks_processed": 0,
                "adapters": [],
                "poll_interval": 5,
            }

    return router


# ── Internal helpers ──────────────────────────────────────────────────────────

def _record_event(db, task_id: str, actor: str, event_type: str, *,
                   summary: str | None = None,
                   content: str | None = None,
                   metadata_json: str | None = None) -> AgentEvent:
    """Create and flush an AgentEvent. Caller must commit the session."""
    event = AgentEvent(
        id=str(uuid.uuid4()),
        task_id=task_id,
        actor=actor,
        event_type=event_type,
        summary=summary,
        content=content,
        metadata_json=metadata_json,
    )
    db.add(event)
    db.flush()
    return event


def _count_tasks() -> int:
    """Total Agent Hub task count (best-effort, never raises)."""
    try:
        db = SessionLocal()
        try:
            return db.query(AgentTask).count()
        finally:
            db.close()
    except Exception:
        return 0
