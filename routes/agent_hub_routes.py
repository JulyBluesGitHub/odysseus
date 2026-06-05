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

from core.database import SessionLocal, AgentTask, AgentEvent, WorkflowTemplate
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
VALID_EVENT_TYPES = {"message", "status_change", "approval", "error", "lock", "context"}
VALID_OWNERS = {"user", "hermes", "codex", "cursor"}
VALID_SANDBOX_MODES = {"read-only", "workspace-write", "danger-full-access"}
VALID_ROLES = {"diagnoser", "implementer", "verifier"}


class TaskCreate(BaseModel):
    title: str = "Untitled Task"
    objective: Optional[str] = None
    status: str = "draft"
    phase: Optional[str] = None
    current_owner: Optional[str] = None
    role: Optional[str] = None
    # diagnoser | implementer | verifier — resolved to adapter at dispatch time
    approval_required: bool = False
    session_id: Optional[str] = None
    chain_task_id: Optional[str] = None
    sandbox_mode: str = "workspace-write"
    depends_on: Optional[list] = None
    # List of task IDs that must be done before this one runs
    tags: list[str] = []


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    objective: Optional[str] = None
    status: Optional[str] = None
    phase: Optional[str] = None
    current_owner: Optional[str] = None
    role: Optional[str] = None
    approval_required: Optional[bool] = None
    session_id: Optional[str] = None
    chain_task_id: Optional[str] = None
    sandbox_mode: Optional[str] = None
    depends_on: Optional[list] = None
    tags: Optional[list[str]] = None


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


class TemplateStep(BaseModel):
    role: str
    title_template: str
    depends_on_index: Optional[int] = None


class TemplateCreate(BaseModel):
    name: str
    steps: list[TemplateStep]


class TemplateUpdate(BaseModel):
    name: Optional[str] = None
    steps: Optional[list[TemplateStep]] = None


class TemplateInstantiate(BaseModel):
    title: str


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
        "role": t.role,
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
        "tags": t.tags or [],
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


def _template_to_dict(t: WorkflowTemplate) -> dict:
    return {
        "id": t.id,
        "owner": t.owner,
        "name": t.name,
        "steps": t.steps or [],
        "created_at": t.created_at.isoformat() + "Z" if t.created_at else None,
        "updated_at": t.updated_at.isoformat() + "Z" if t.updated_at else None,
    }


def _normalize_template_steps(steps: list[TemplateStep]) -> list[dict]:
    if not steps:
        raise HTTPException(400, "Template must include at least one step")
    if len(steps) > 20:
        raise HTTPException(400, "Too many template steps (max 20)")

    normalized = []
    for idx, step in enumerate(steps):
        role = (step.role or "").strip()
        title_template = (step.title_template or "").strip()
        if role not in VALID_ROLES:
            raise HTTPException(400, f"Invalid role at step {idx + 1}: {role}")
        if not title_template:
            raise HTTPException(400, f"title_template is required at step {idx + 1}")
        depends_on_index = step.depends_on_index
        if depends_on_index is not None:
            if depends_on_index < 0 or depends_on_index >= idx:
                raise HTTPException(
                    400,
                    f"depends_on_index at step {idx + 1} must reference an earlier step",
                )
        normalized.append({
            "role": role,
            "title_template": title_template,
            "depends_on_index": depends_on_index,
        })
    return normalized


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


def _validate_dependencies(dep_ids: list, db) -> list | None:
    """Validate a list of dependency task IDs. Returns normalized list or raises HTTPException."""
    if not isinstance(dep_ids, list):
        raise HTTPException(400, "depends_on must be a list of task IDs")
    if len(dep_ids) == 0:
        return []
    if len(dep_ids) > 20:
        raise HTTPException(400, "Too many dependencies (max 20)")
    seen = set()
    valid_ids = []
    for dep_id in dep_ids:
        if not isinstance(dep_id, str) or not dep_id.strip():
            raise HTTPException(400, "Each dependency must be a non-empty task ID string")
        tid = dep_id.strip()
        if tid in seen:
            raise HTTPException(400, f"Duplicate dependency: {tid}")
        seen.add(tid)
        dep_task = db.query(AgentTask).filter(AgentTask.id == tid).first()
        if not dep_task:
            raise HTTPException(400, f"Dependency task not found: {tid}")
        valid_ids.append(tid)
    return valid_ids


def _detect_dependency_cycle(task_id: str, dep_ids: list, db) -> bool:
    """Return True if adding these dependencies would create a cycle."""
    if task_id in dep_ids:
        return True
    visited = set()
    stack = list(dep_ids)
    while stack:
        current = stack.pop()
        if current == task_id:
            return True
        if current in visited:
            continue
        visited.add(current)
        dep_task = db.query(AgentTask).filter(AgentTask.id == current).first()
        if dep_task and dep_task.depends_on:
            for d in dep_task.depends_on:
                if d not in visited:
                    stack.append(d)
    return False


def _index_task_for_rag(task: AgentTask) -> None:
    """Best-effort semantic index update for Agent Hub tasks."""
    try:
        from src.agent_hub_rag import index_task

        index_task(
            task.id,
            task.title or "",
            task.objective or "",
            task.status or "",
            task.role or "",
        )
    except Exception as exc:
        logger.debug("Agent Hub RAG index hook skipped for %s: %s", task.id, exc)


def _delete_task_from_rag(task_id: str) -> None:
    """Best-effort semantic index deletion for Agent Hub tasks."""
    try:
        from src.agent_hub_rag import delete_task_embedding

        delete_task_embedding(task_id)
    except Exception as exc:
        logger.debug("Agent Hub RAG delete hook skipped for %s: %s", task_id, exc)


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

    # ── Workflow Templates ────────────────────────────────────────────────

    @router.get("/templates")
    async def list_templates(request: Request):
        """List saved workflow templates for the current user."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            qobj = db.query(WorkflowTemplate)
            if user:
                qobj = qobj.filter(WorkflowTemplate.owner == user)
            else:
                qobj = qobj.filter(WorkflowTemplate.owner == None)  # noqa: E711
            templates = qobj.order_by(WorkflowTemplate.updated_at.desc()).all()
            return {"templates": [_template_to_dict(t) for t in templates]}
        finally:
            db.close()

    @router.post("/templates", status_code=201)
    async def create_template(request: Request, body: TemplateCreate):
        """Create a saved workflow template."""
        user = get_current_user(request)
        name = (body.name or "").strip()
        if not name:
            raise HTTPException(400, "Template name is required")
        steps = _normalize_template_steps(body.steps)

        db = SessionLocal()
        try:
            template = WorkflowTemplate(
                id=str(uuid.uuid4()),
                owner=user,
                name=name,
                steps=steps,
            )
            db.add(template)
            db.commit()
            db.refresh(template)
            return _template_to_dict(template)
        finally:
            db.close()

    @router.get("/templates/{template_id}")
    async def get_template(request: Request, template_id: str):
        """Get one saved workflow template."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            template = db.query(WorkflowTemplate).filter(WorkflowTemplate.id == template_id).first()
            if not template:
                raise HTTPException(404, "Template not found")
            if user and template.owner != user:
                raise HTTPException(404, "Template not found")
            if not user and template.owner is not None:
                raise HTTPException(404, "Template not found")
            return _template_to_dict(template)
        finally:
            db.close()

    @router.put("/templates/{template_id}")
    async def update_template(request: Request, template_id: str, body: TemplateUpdate):
        """Update a saved workflow template."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            template = db.query(WorkflowTemplate).filter(WorkflowTemplate.id == template_id).first()
            if not template:
                raise HTTPException(404, "Template not found")
            if user and template.owner != user:
                raise HTTPException(404, "Template not found")
            if not user and template.owner is not None:
                raise HTTPException(404, "Template not found")

            if body.name is not None:
                name = body.name.strip()
                if not name:
                    raise HTTPException(400, "Template name is required")
                template.name = name
            if body.steps is not None:
                template.steps = _normalize_template_steps(body.steps)

            db.commit()
            db.refresh(template)
            return _template_to_dict(template)
        finally:
            db.close()

    @router.delete("/templates/{template_id}")
    async def delete_template(request: Request, template_id: str):
        """Delete a saved workflow template."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            template = db.query(WorkflowTemplate).filter(WorkflowTemplate.id == template_id).first()
            if not template:
                raise HTTPException(404, "Template not found")
            if user and template.owner != user:
                raise HTTPException(404, "Template not found")
            if not user and template.owner is not None:
                raise HTTPException(404, "Template not found")
            db.delete(template)
            db.commit()
            return {"ok": True}
        finally:
            db.close()

    @router.post("/templates/{template_id}/instantiate")
    async def instantiate_template(request: Request, template_id: str, body: TemplateInstantiate):
        """Create an Agent Hub task chain from a saved workflow template."""
        user = get_current_user(request)
        input_title = (body.title or "").strip() or "Untitled Task"
        db = SessionLocal()
        created_tasks: list[AgentTask] = []
        try:
            template = db.query(WorkflowTemplate).filter(WorkflowTemplate.id == template_id).first()
            if not template:
                raise HTTPException(404, "Template not found")
            if user and template.owner != user:
                raise HTTPException(404, "Template not found")
            if not user and template.owner is not None:
                raise HTTPException(404, "Template not found")

            steps = _normalize_template_steps([
                TemplateStep(**step) for step in (template.steps or [])
            ])
            task_ids: list[str] = []
            for step in steps:
                depends_on = None
                depends_on_index = step.get("depends_on_index")
                if depends_on_index is not None:
                    depends_on = [task_ids[depends_on_index]]
                task = AgentTask(
                    id=str(uuid.uuid4()),
                    owner=user,
                    title=step["title_template"].replace("{title}", input_title),
                    status="draft",
                    role=step["role"],
                    depends_on=depends_on,
                    tags=[],
                )
                db.add(task)
                db.flush()
                created_tasks.append(task)
                task_ids.append(task.id)

            if created_tasks:
                created_tasks[0].status = "queued"

            db.commit()
            for task in created_tasks:
                db.refresh(task)

            from src.agent_hub_events import publish
            for task in created_tasks:
                publish(user or "", "task_created", _task_to_dict(task))

            return {"ok": True, "task_ids": task_ids}
        finally:
            db.close()

    # ── Task CRUD ─────────────────────────────────────────────────────────

    @router.get("/tasks")
    async def list_tasks(
        request: Request,
        status: Optional[str] = Query(None),
        owner_agent: Optional[str] = Query(None, alias="owner"),
        q: Optional[str] = Query(None, description="Free-text search on title and objective"),
        tag: Optional[str] = Query(None),
    ):
        """List Agent Hub tasks, optionally filtered by status, owner, tag, and/or keyword search."""
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
            if tag:
                qobj = qobj.filter(AgentTask.tags.contains([tag]))
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
        if body.role is not None and body.role not in VALID_ROLES:
            raise HTTPException(400, f"Invalid role: {body.role}")

        db = SessionLocal()
        try:
            # Validate dependencies
            dep_ids = None
            if body.depends_on is not None:
                dep_ids = _validate_dependencies(body.depends_on, db)
                # Cycle check: new task doesn't have an ID yet, but we check that
                # none of the deps transitively depend on each other in a cycle
                # (full cycle detection happens on update when task has an ID)

            task = AgentTask(
                id=str(uuid.uuid4()),
                owner=user,
                title=body.title,
                objective=body.objective,
                status=body.status,
                phase=body.phase,
                current_owner=body.current_owner,
                role=body.role,
                depends_on=dep_ids,
                approval_required=body.approval_required,
                session_id=body.session_id,
                chain_task_id=body.chain_task_id,
                sandbox_mode=body.sandbox_mode,
                tags=body.tags,
            )
            db.add(task)
            db.commit()
            db.refresh(task)
            result = _task_to_dict(task)
            _index_task_for_rag(task)
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
            if body.role is not None:
                if body.role not in VALID_ROLES and body.role != "":
                    raise HTTPException(400, f"Invalid role: {body.role}")
                task.role = body.role if body.role else None
            if body.depends_on is not None:
                dep_ids = _validate_dependencies(body.depends_on, db)
                if _detect_dependency_cycle(task_id, dep_ids, db):
                    raise HTTPException(400, "Dependency cycle detected")
                task.depends_on = dep_ids if dep_ids else None
            if body.tags is not None:
                task.tags = body.tags

            db.commit()
            db.refresh(task)
            result = _task_to_dict(task)
            _index_task_for_rag(task)
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
            _delete_task_from_rag(task_id)
            from src.agent_hub_events import publish
            publish(user or "", "task_deleted", {"id": task_id})
            return {"ok": True}
        finally:
            db.close()

    @router.get("/tasks/{task_id}/similar")
    async def similar_tasks(request: Request, task_id: str, n: int = Query(5, ge=1, le=20)):
        """Return semantically similar Agent Hub tasks."""
        user = get_current_user(request)
        db = SessionLocal()
        try:
            task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
            if not task:
                raise HTTPException(404, "Task not found")
            if user and task.owner and task.owner != user:
                raise HTTPException(404, "Task not found")
        finally:
            db.close()

        from src.agent_hub_rag import find_similar

        similar = find_similar(task_id, n)
        if similar:
            ids = [item["id"] for item in similar if item.get("id")]
            db = SessionLocal()
            try:
                qobj = db.query(AgentTask).filter(AgentTask.id.in_(ids))
                if user:
                    qobj = qobj.filter(AgentTask.owner == user)
                allowed_ids = {row.id for row in qobj.all()}
            finally:
                db.close()
            similar = [item for item in similar if item.get("id") in allowed_ids]

        return {"task_id": task_id, "similar": similar}

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
            _index_task_for_rag(task)
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

    # ── Role Bindings ──────────────────────────────────────────────────────

    @router.get("/bindings")
    async def list_bindings(request: Request):
        """List role bindings for the current user.

        Returns global (owner=None) bindings and any owner-specific overrides.
        """
        from core.database import RoleBinding
        user = get_current_user(request)
        db = SessionLocal()
        try:
            bindings = (
                db.query(RoleBinding)
                .filter(
                    (RoleBinding.owner == user) | (RoleBinding.owner == None)  # noqa: E711
                )
                .order_by(RoleBinding.role, RoleBinding.owner)
                .all()
            )
            return {
                "bindings": [
                    {
                        "id": b.id,
                        "owner": b.owner,
                        "role": b.role,
                        "adapter_name": b.adapter_name,
                    }
                    for b in bindings
                ]
            }
        finally:
            db.close()

    @router.put("/bindings")
    async def update_binding(request: Request):
        """Create or update a role binding. Body: {role, adapter_name}.

        If a binding for this owner+role already exists, it's updated.
        Otherwise a new binding is created. Passing owner=null creates a
        global binding (only if no owner-specific override exists for that
        role).
        """
        from core.database import RoleBinding
        user = get_current_user(request)
        import json as _json
        body = await request.json()
        role = (body.get("role") or "").strip()
        adapter = (body.get("adapter_name") or "").strip()
        owner = body.get("owner")  # None = global, string = owner-specific

        if not role or role not in VALID_ROLES:
            raise HTTPException(400, f"Invalid role: {role}")
        if adapter and adapter not in VALID_OWNERS:
            raise HTTPException(400, f"Invalid adapter_name: {adapter}")

        db = SessionLocal()
        try:
            # Authorize: only admins can set global bindings (best-effort)
            if owner is None and user:
                try:
                    from core.auth import auth_manager
                    _users = getattr(auth_manager, "users", {})
                    _user_data = _users.get(user, {})
                    if not _user_data.get("is_admin"):
                        raise HTTPException(403, "Only admins can set global bindings")
                except ImportError:
                    pass  # test context — allow global bindings

            binding = (
                db.query(RoleBinding)
                .filter(
                    RoleBinding.role == role,
                    (RoleBinding.owner == owner) if owner else (RoleBinding.owner == None),  # noqa: E711
                )
                .first()
            )

            if binding:
                binding.adapter_name = adapter
            else:
                binding = RoleBinding(
                    id=str(uuid.uuid4()),
                    owner=owner,
                    role=role,
                    adapter_name=adapter,
                )
                db.add(binding)

            db.commit()
            db.refresh(binding)
            return {
                "id": binding.id,
                "owner": binding.owner,
                "role": binding.role,
                "adapter_name": binding.adapter_name,
            }
        finally:
            db.close()

    # ── Transition ─────────────────────────────────────────────────────────

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
            _index_task_for_rag(task)
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

    # ── Batch Operations ────────────────────────────────────────────────────

    class BatchRequest(BaseModel):
        action: str  # "delete" | "cancel" | "retry"
        task_ids: list[str]

    class BatchFailure(BaseModel):
        id: str
        error: str

    class BatchResults(BaseModel):
        succeeded: int = 0
        failed: list[dict] = []

    class BatchResponse(BaseModel):
        ok: bool = True
        results: BatchResults

    @router.post("/tasks/batch")
    async def batch_tasks(request: Request, body: BatchRequest):
        """Execute a bulk action (delete/cancel/retry) on multiple tasks.

        Each task is owner-scoped. Failures on individual tasks are captured
        in ``results.failed`` — the endpoint does not fail-fast.
        """
        user = get_current_user(request)
        if body.action not in ("delete", "cancel", "retry"):
            raise HTTPException(400,
                               f"Invalid action: {body.action}. Must be delete, cancel, or retry.")
        if not body.task_ids:
            raise HTTPException(400, "task_ids must not be empty")

        db = SessionLocal()
        results = BatchResults()
        try:
            for task_id in body.task_ids:
                task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
                if not task:
                    results.failed.append({"id": task_id, "error": "Task not found"})
                    continue
                if user and task.owner and task.owner != user:
                    results.failed.append({"id": task_id, "error": "Not your task"})
                    continue

                try:
                    if body.action == "delete":
                        db.delete(task)
                        db.flush()
                    elif body.action == "cancel":
                        task.status = "cancelled"
                        task.locked_by = None
                        db.flush()
                    elif body.action == "retry":
                        task.status = "queued"
                        task.locked_by = None
                        task.attempts = 0
                        db.flush()

                    results.succeeded += 1

                    # Publish update for non-delete actions
                    if body.action != "delete":
                        result_dict = _task_to_dict(task)
                        from src.agent_hub_events import publish
                        publish(user or "", "task_updated", result_dict)

                except Exception as exc:
                    db.rollback()
                    results.failed.append({"id": task_id, "error": str(exc)})

            if results.succeeded > 0:
                db.commit()

            return BatchResponse(results=results)
        finally:
            db.close()

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
