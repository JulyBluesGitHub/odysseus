"""Agent Hub coordinator — background loop that moves tasks between agents.

Pattern-matched on ``src/bg_monitor.py``: an always-on asyncio loop that polls
for queued tasks, claims them atomically, dispatches to the appropriate adapter,
records the result as an event, and transitions the task state.

Gate: set ``AGENT_HUB_ENABLED=false`` to disable at startup. Default is enabled.
Poll interval: ``AGENT_HUB_POLL_INTERVAL`` seconds (default 5).

Architecture
------------
1. Poll: SELECT tasks WHERE status='queued' AND current_owner IS NOT NULL AND locked_by IS NULL
2. Claim: SET locked_by=<adapter>, locked_at=NOW() — one coordinator wins
3. Dispatch: call adapter.run(task, events)
4. Record: write event with adapter output
5. Transition: apply proposed_status / proposed_owner / needs_approval
6. Unlock: clear locked_by/locked_at
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone

from core.database import SessionLocal, AgentTask, AgentEvent
from src.agent_hub_events import publish as _publish_event
from sqlalchemy import or_

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

POLL_INTERVAL = float(os.getenv("AGENT_HUB_POLL_INTERVAL", "5"))
"""Seconds between coordinator ticks."""

MAX_ATTEMPTS = 3
"""Max dispatch attempts before a task is marked blocked."""

# ── Concurrency caps ──────────────────────────────────────────────────────────

CAPS = {
    "global": int(os.getenv("AGENT_HUB_CAP_GLOBAL", "3")),
    "adapter": {
        k: int(v) for k, v in
        (item.split(":") for item in os.getenv(
            "AGENT_HUB_CAP_ADAPTER", "codex:1,hermes:2"
        ).split(","))
    },
    "role": {
        k: int(v) for k, v in
        (item.split(":") for item in os.getenv(
            "AGENT_HUB_CAP_ROLE", "implementer:1,verifier:2"
        ).split(","))
    },
}
"""Concurrency limits. Global cap applies first, then per-adapter and per-role
caps are checked independently. A task must satisfy all applicable caps."""

# Running counts — incremented when a task is claimed, decremented on completion
_running_by_adapter: dict[str, int] = {}
_running_by_role: dict[str, int] = {}
_running_total = 0


def _publish_task(task_id: str) -> None:
    """Re-fetch a task from DB and publish a task_updated event to its owner."""
    from src.agent_hub_events import _task_to_ssedict
    db = SessionLocal()
    try:
        task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
        if task:
            _publish_event(task.owner or "", "task_updated", _task_to_ssedict(task))
    finally:
        db.close()


# ── Transition table ──────────────────────────────────────────────────────────

# Valid status transitions the coordinator is allowed to make.
# Mirrors TRANSITIONS in routes/agent_hub_routes.py but coordinator-only.
_COORDINATOR_TRANSITIONS: dict[str, set[str]] = {
    "queued":                {"running"},
    "running":               {"waiting_for_approval", "queued", "blocked", "done"},
    "waiting_for_approval":  {"queued"},   # approval comes from the user via API
    "blocked":               {"queued"},
}


def _coordinator_can_transition(current: str, proposed: str) -> bool:
    """Check whether the coordinator is allowed to make this transition."""
    allowed = _COORDINATOR_TRANSITIONS.get(current, set())
    return proposed in allowed


# ── Role resolution ────────────────────────────────────────────────────────────

VALID_ROLES = {"diagnoser", "implementer", "verifier"}


def _resolve_role(role: str, task_owner: str | None) -> str | None:
    """Look up the adapter bound to a role for a given owner.

    Checks owner-specific bindings first, then global (owner=None) bindings.
    Returns the adapter_name (hermes, codex, cursor) or None if no binding exists.
    """
    from core.database import SessionLocal as _SL, RoleBinding
    db = _SL()
    try:
        # Owner-specific binding takes priority
        if task_owner:
            binding = (
                db.query(RoleBinding)
                .filter(RoleBinding.role == role, RoleBinding.owner == task_owner)
                .first()
            )
            if binding:
                return binding.adapter_name
        # Fall back to global binding
        binding = (
            db.query(RoleBinding)
            .filter(RoleBinding.role == role, RoleBinding.owner == None)  # noqa: E711
            .first()
        )
        return binding.adapter_name if binding else None
    finally:
        db.close()


def _get_unmet_dependencies(task, db) -> list:
    """Return list of dep IDs that are NOT done. Empty list = all deps satisfied."""
    if not task.depends_on:
        return []
    unmet = []
    for dep_id in task.depends_on:
        dep_task = db.query(AgentTask).filter(AgentTask.id == dep_id).first()
        if not dep_task or dep_task.status != "done":
            unmet.append(dep_id)
    return unmet


def _block_task(db, task, reason: str):
    """Mark a task as blocked with an error event and commit."""
    task.status = "blocked"
    task.last_error = reason
    _record_event(
        db, task.id, "coordinator", "error",
        summary=reason,
    )
    db.flush()  # keep in current transaction — caller commits


def _execute_create_task_actions(db, parent_task, actions: list) -> int:
    """Execute any create_task actions from an adapter result.

    Creates subtasks in the DB, records timeline events on both parent and
    child, and returns the number of tasks created.
    """
    import uuid as _uuid
    created = 0
    for action in actions:
        if action.type != "create_task":
            continue
        if not action.role or action.role not in {"diagnoser", "implementer", "verifier"}:
            _record_event(
                db, parent_task.id, "coordinator", "error",
                summary=f"Skipped create_task: invalid or missing role '{action.role}'",
            )
            continue

        child_id = str(_uuid.uuid4())
        child = AgentTask(
            id=child_id,
            owner=parent_task.owner,
            title=action.task_title or f"Subtask of {parent_task.title[:40]}",
            objective=action.objective or "",
            status="queued",
            role=action.role,
            current_owner=None,  # resolved at claim time via role binding
            depends_on=list(action.depends_on) if action.depends_on else None,
            created_by_task_id=parent_task.id,
            sandbox_mode=parent_task.sandbox_mode,
            session_id=parent_task.session_id,
        )
        db.add(child)

        # Timeline: parent
        _record_event(
            db, parent_task.id, "coordinator", "status_change",
            summary=f"Created subtask '{child.title}' for role {child.role}",
        )
        # Timeline: child
        _record_event(
            db, child_id, "coordinator", "status_change",
            summary=(
                f"Created by task '{parent_task.title}'"
                + (f" (waiting on {len(action.depends_on)} dependencies)" if action.depends_on else "")
            ),
        )
        created += 1
        logger.info(
            "Agent Hub: subtask '%s' (role=%s) created by task %s",
            child.title, child.role, parent_task.id,
        )

    if created:
        db.commit()
    return created


# ── Coordinator state ─────────────────────────────────────────────────────────

_coordinator_task: asyncio.Task | None = None
_running = False
_last_tick: datetime | None = None
_tasks_processed = 0
_adapter_registry: dict[str, object] = {}  # owner_name → adapter instance


# ── Public API ────────────────────────────────────────────────────────────────

def register_adapter(owner_name: str, adapter):
    """Register an adapter for a given owner name (e.g. 'hermes', 'codex').

    The coordinator maps ``AgentTask.current_owner`` to the adapter instance
    that handles it. Only registered owners are dispatched.
    """
    _adapter_registry[owner_name] = adapter
    logger.info("Agent Hub: registered adapter for '%s'", owner_name)


async def start():
    """Start the coordinator background loop. Idempotent."""
    global _coordinator_task, _running
    if _running:
        return
    if os.getenv("AGENT_HUB_ENABLED", "true").lower() in ("0", "false", "no", "off"):
        logger.info("Agent Hub coordinator disabled (AGENT_HUB_ENABLED=false)")
        return
    _running = True
    _coordinator_task = asyncio.create_task(_coordinator_loop())
    logger.info(
        "Agent Hub coordinator started (poll=%.0fs, adapters=%d)",
        POLL_INTERVAL, len(_adapter_registry),
    )


async def stop():
    """Stop the coordinator loop. Idempotent. Releases all locks."""
    global _coordinator_task, _running
    _running = False
    if _coordinator_task:
        _coordinator_task.cancel()
        try:
            await _coordinator_task
        except asyncio.CancelledError:
            pass
        _coordinator_task = None
    _release_all_locks()
    logger.info("Agent Hub coordinator stopped")


def is_running() -> bool:
    return _running


def get_status() -> dict:
    """Lightweight status snapshot for GET /api/agent-hub/status."""
    return {
        "running": _running,
        "last_tick": _last_tick.isoformat() + "Z" if _last_tick else None,
        "tasks_processed": _tasks_processed,
        "adapters": sorted(_adapter_registry.keys()),
        "poll_interval": POLL_INTERVAL,
        "caps": CAPS,
        "running_counts": {
            "total": _running_total,
            "by_adapter": dict(_running_by_adapter),
            "by_role": dict(_running_by_role),
        },
    }


# ── Cap helpers ────────────────────────────────────────────────────────────────

def _inc_running(adapter: str, role: str | None):
    global _running_total
    _running_total += 1
    _running_by_adapter[adapter] = _running_by_adapter.get(adapter, 0) + 1
    if role:
        _running_by_role[role] = _running_by_role.get(role, 0) + 1


def _dec_running(adapter: str, role: str | None):
    global _running_total
    _running_total = max(0, _running_total - 1)
    _running_by_adapter[adapter] = max(0, _running_by_adapter.get(adapter, 0) - 1)
    if role:
        _running_by_role[role] = max(0, _running_by_role.get(role, 0) - 1)


def _caps_allow(adapter: str, role: str | None) -> bool:
    """Check if dispatching a task to this adapter/role would exceed any cap."""
    if _running_total >= CAPS["global"]:
        return False
    cap_adapter = CAPS["adapter"].get(adapter)
    if cap_adapter is not None and _running_by_adapter.get(adapter, 0) >= cap_adapter:
        return False
    if role:
        cap_role = CAPS["role"].get(role)
        if cap_role is not None and _running_by_role.get(role, 0) >= cap_role:
            return False
    return True


# ── Context briefs ─────────────────────────────────────────────────────────────

# Brief generation limits
_MAX_SIMILAR_TASKS = 5
_MAX_DEP_EVENTS = 8
_MAX_EVENT_CONTENT_CHARS = 500
_MAX_BRIEF_CHARS = 6000

# Cache of last fingerprint per task to detect duplicates
_last_brief_fingerprints: dict[str, str] = {}


def _brief_fingerprint(task) -> str:
    """Compute a fingerprint for dedup: task ID + role + dep task IDs + dep statuses."""
    import hashlib
    parts = [task.id, task.role or "", task.status]
    if task.depends_on:
        parts.extend(sorted(task.depends_on))
        # Include dependency statuses so brief regenerates when deps complete
        dep_statuses = _get_dep_statuses(task)
        parts.extend(dep_statuses)
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _get_dep_statuses(task) -> list:
    """Get status strings for each dependency task."""
    from core.database import SessionLocal as _SL
    if not task.depends_on:
        return []
    db = _SL()
    try:
        dep_tasks = db.query(AgentTask).filter(AgentTask.id.in_(task.depends_on)).all()
        return sorted(f"{t.id}:{t.status}" for t in dep_tasks)
    finally:
        db.close()


def _brief_similar_tasks(db, task) -> list[dict]:
    """Find up to _MAX_SIMILAR_TASKS past tasks similar to this one.

    Excludes the task itself, its dependency chain, and prior context events.
    Owner-scoped. Matches on title keywords.
    """
    if not task.title:
        return []
    # Extract keywords (words > 3 chars)
    keywords = [w.lower() for w in task.title.split() if len(w) > 3]
    if not keywords:
        return []

    # Build a rough ILIKE filter
    from sqlalchemy import or_ as _or
    filters = []
    for kw in keywords[:5]:  # max 5 keywords
        filters.append(AgentTask.title.ilike(f"%{kw}%"))
        filters.append(AgentTask.objective.ilike(f"%{kw}%"))

    # Exclude self and dependency chain
    exclude_ids = {task.id}
    if task.depends_on:
        exclude_ids.update(task.depends_on)

    similar = (
        db.query(AgentTask)
        .filter(
            AgentTask.owner == task.owner,
            AgentTask.id.notin_(exclude_ids),
            _or(*filters),
        )
        .order_by(AgentTask.updated_at.desc())
        .limit(_MAX_SIMILAR_TASKS)
        .all()
    )
    return [
        {
            "id": t.id,
            "title": t.title,
            "status": t.status,
            "role": t.role,
            "summary": (t.objective or "")[:200],
        }
        for t in similar
    ]


def _brief_dependency_events(db, task) -> list[dict]:
    """Extract up to _MAX_DEP_EVENTS events from dependency tasks.

    Skips context events to avoid recursive bloat. Returns newest first.
    """
    if not task.depends_on:
        return []
    events = (
        db.query(AgentEvent)
        .filter(
            AgentEvent.task_id.in_(task.depends_on),
            AgentEvent.event_type != "context",
        )
        .order_by(AgentEvent.created_at.desc())
        .limit(_MAX_DEP_EVENTS)
        .all()
    )
    return [
        {
            "task_id": e.task_id,
            "actor": e.actor,
            "event_type": e.event_type,
            "summary": e.summary or "",
            "content": (e.content or "")[:_MAX_EVENT_CONTENT_CHARS],
        }
        for e in events
    ]


def _build_role_context_brief(db, task) -> str | None:
    """Build a role-specific context brief. Returns None if identical to last brief."""
    import hashlib

    fp = _brief_fingerprint(task)
    if _last_brief_fingerprints.get(task.id) == fp:
        return None  # fingerprint unchanged — skip duplicate

    role = task.role or ""
    lines = [f"# Context Brief — {role or 'General'}", ""]

    # ── Common: task info ──
    lines.append(f"**Task:** {task.title}")
    if task.objective:
        lines.append(f"**Objective:** {task.objective}")
    lines.append("")

    # ── Role-specific sections ──
    if role == "diagnoser":
        lines.append("## Similar Past Tasks")
        similar = _brief_similar_tasks(db, task)
        if similar:
            for s in similar:
                lines.append(f"- [{s['status']}] {s['title']} (role: {s.get('role', 'none')})")
                if s.get("summary"):
                    lines.append(f"  {s['summary'][:120]}")
        else:
            lines.append("(none found)")
        lines.append("")

        # Include recent error events from deps
        dep_events = _brief_dependency_events(db, task)
        errors = [e for e in dep_events if e["event_type"] == "error"]
        if errors:
            lines.append("## Recent Errors from Dependencies")
            for e in errors[:3]:
                lines.append(f"- {e['summary']}")

    elif role == "implementer":
        lines.append("## Diagnosis Summary")
        dep_events = _brief_dependency_events(db, task)
        msgs = [e for e in dep_events if e["event_type"] in ("message", "status_change")]
        if msgs:
            for e in msgs[:_MAX_DEP_EVENTS]:
                lines.append(f"- [{e['actor']}] {e['summary']}")
                if e.get("content"):
                    lines.append(f"  {e['content'][:200]}")
        else:
            lines.append("(no diagnosis from dependencies)")
        lines.append("")

        if task.objective:
            lines.append("## Acceptance Criteria")
            lines.append(task.objective[:_MAX_BRIEF_CHARS // 3])
        lines.append("")

    elif role == "verifier":
        lines.append("## Original Objective")
        dep_events = _brief_dependency_events(db, task)
        msgs = [e for e in dep_events if e["event_type"] in ("message", "status_change", "error")]
        if msgs:
            for e in msgs[:4]:
                lines.append(f"- [{e['actor']}] {e['summary']}")
        lines.append("")

        lines.append("## Implementation Evidence")
        # Look for shell/file actions, test output, status changes
        evidence = [e for e in dep_events
                    if e["event_type"] in ("message", "status_change")
                    or (e.get("content") and any(
                        kw in (e.get("content") or "").lower()
                        for kw in ("test", "pass", "fail", "build", "compile", "error")
                    ))]
        if evidence:
            for e in evidence[:6]:
                lines.append(f"- [{e['actor']}] {e['summary']}")
                if e.get("content"):
                    lines.append(f"  {e['content'][:200]}")
        else:
            lines.append("(no implementation evidence from dependencies)")
    else:
        # No role — generic brief
        if task.depends_on:
            lines.append("## Dependencies")
            dep_statuses = _get_dep_statuses(task)
            for ds in dep_statuses:
                lines.append(f"- {ds}")

    brief = "\n".join(lines)[:_MAX_BRIEF_CHARS]
    _last_brief_fingerprints[task.id] = fp
    return brief


def _inject_context_brief(db, task):
    """Build and inject a role-specific context brief event. Idempotent."""
    brief = _build_role_context_brief(db, task)
    if not brief:
        return  # fingerprint unchanged
    _record_event(
        db, task.id, "coordinator", "context",
        summary=f"Role-specific context brief ({task.role or 'general'})",
        content=brief,
        metadata_json=_safe_json_dump({
            "kind": "role_context_brief",
            "role": task.role,
            "fingerprint": _last_brief_fingerprints.get(task.id, ""),
        }),
    )


# ── Loop ──────────────────────────────────────────────────────────────────────

async def _coordinator_loop():
    global _last_tick, _tasks_processed
    # Probe all adapters at startup — log unavailable ones
    for name, adapter in _adapter_registry.items():
        try:
            probe = await adapter.probe()
            if not probe.available:
                logger.warning(
                    "Agent Hub: adapter '%s' unavailable: %s", name, probe.error
                )
        except Exception as exc:
            logger.warning(
                "Agent Hub: adapter '%s' probe failed: %s", name, exc
            )

    while _running:
        try:
            _last_tick = datetime.now(timezone.utc)
            await _tick()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Agent Hub coordinator tick failed")
        await asyncio.sleep(POLL_INTERVAL)


async def _tick():
    """Coordinator tick — claim and dispatch all eligible queued tasks.

    Resolves roles, checks dependencies, and enforces concurrency caps
    (global, per-adapter, per-role). Eligible tasks are dispatched in
    parallel via asyncio.gather.
    """
    global _tasks_processed

    db = SessionLocal()
    try:
        # Find ALL queued, unlocked tasks sorted by creation time
        candidates = (
            db.query(AgentTask)
            .filter(
                AgentTask.status == "queued",
                AgentTask.locked_by.is_(None),
                or_(
                    AgentTask.current_owner.in_(_adapter_registry.keys()),
                    AgentTask.role.isnot(None),
                ),
            )
            .order_by(AgentTask.created_at)
            .limit(CAPS["global"] + 5)  # fetch a few more than global cap
            .all()
        )
        if not candidates:
            return

        # Filter: resolve roles, check deps, check caps
        eligible = []
        for task in candidates:
            # Resolve role
            if task.role:
                if task.role not in VALID_ROLES:
                    _block_task(db, task, f"Invalid role: {task.role}")
                    continue
                resolved = _resolve_role(task.role, task.owner)
                if not resolved:
                    _block_task(db, task, f"No adapter bound to role '{task.role}'")
                    continue
                if getattr(task, "current_owner", None) != resolved:
                    old_owner = task.current_owner
                    task.current_owner = resolved
                    _record_event(
                        db, task.id, "coordinator", "status_change",
                        summary=f"Resolved role {task.role} to adapter {resolved}"
                        + (f" (was {old_owner})" if old_owner and old_owner != resolved else ""),
                    )

            # Check dependencies
            if task.depends_on and _get_unmet_dependencies(task, db):
                continue  # skip — still waiting

            # Check caps
            adapter = task.current_owner
            if adapter not in _adapter_registry:
                continue
            if not _caps_allow(adapter, task.role):
                continue  # at capacity — try next tick

            eligible.append(task)
            _inc_running(adapter, task.role)

            if len(eligible) >= CAPS["global"]:
                break

        # Claim all eligible tasks
        for task in eligible:
            # Inject role-specific context brief BEFORE claiming
            _inject_context_brief(db, task)
            task.locked_by = task.current_owner
            task.locked_at = datetime.now(timezone.utc)
            task.attempt_count = (task.attempt_count or 0) + 1
            task.status = "running"
            _record_event(
                db, task.id, "coordinator", "lock",
                summary=f"Claimed by {task.current_owner} (attempt {task.attempt_count})",
            )

        db.commit()
        task_ids = [t.id for t in eligible]
        for tid in task_ids:
            _publish_task(tid)
    finally:
        db.close()

    if not eligible:
        return

    # Dispatch all in parallel
    async def _process_one(task):
        """Claim, dispatch, and record result for a single task."""
        global _tasks_processed
        adapter = _adapter_registry[task.current_owner]
        events = _load_events(task.id)
        role = task.role

        try:
            result = await adapter.run(task, events)
        except Exception as exc:
            result = None
            error_msg = f"{type(exc).__name__}: {exc}"

        db2 = SessionLocal()
        try:
            t = db2.query(AgentTask).filter(AgentTask.id == task.id).first()
            if not t:
                return

            if result is None:
                _record_event(
                    db2, t.id, "coordinator", "error",
                    summary=f"Adapter '{task.current_owner}' failed: {error_msg}",
                    content=error_msg,
                )
                if t.attempt_count >= MAX_ATTEMPTS:
                    t.status = "blocked"
                    t.last_error = error_msg
                    _record_event(
                        db2, t.id, "coordinator", "status_change",
                        summary=f"Blocked after {t.attempt_count} failed attempts",
                    )
                else:
                    t.status = "queued"
            else:
                _record_event(
                    db2, t.id, task.current_owner, "message",
                    summary=result.summary,
                    content=result.content,
                    metadata_json=_safe_json_dump(_enrich_metadata(result)),
                )
                _execute_create_task_actions(db2, t, result.actions)

                if result.needs_approval:
                    t.approval_required = True
                    if t.status not in ("done", "cancelled"):
                        t.status = "waiting_for_approval"
                        _record_event(
                            db2, t.id, "coordinator", "status_change",
                            summary="Waiting for user approval",
                        )
                elif result.proposed_status:
                    proposed = result.proposed_status
                    if _coordinator_can_transition(t.status, proposed):
                        old_status = t.status
                        t.status = proposed
                        _record_event(
                            db2, t.id, "coordinator", "status_change",
                            summary=f"Status: {old_status} -> {proposed}",
                        )
                    else:
                        logger.warning(
                            "Agent Hub: adapter '%s' proposed invalid transition '%s' -> '%s' for task %s",
                            task.current_owner, t.status, proposed, t.id,
                        )

                if result.proposed_owner:
                    t.current_owner = result.proposed_owner

            t.locked_by = None
            db2.commit()
            _publish_task(t.id)
            _tasks_processed += 1

            if t.status == "done" and t.chain_task_id:
                _activate_chain(db2, t)
        except Exception:
            logger.exception("Agent Hub: failed to write coordinator result for %s", task.id)
            db2.rollback()
        finally:
            db2.close()
            _dec_running(task.current_owner, role)

    await asyncio.gather(*(_process_one(t) for t in eligible))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_events(task_id: str) -> list:
    """Load events for a task (chronological). Returns empty list on error."""
    try:
        db = SessionLocal()
        try:
            return (
                db.query(AgentEvent)
                .filter(AgentEvent.task_id == task_id)
                .order_by(AgentEvent.created_at)
                .all()
            )
        finally:
            db.close()
    except Exception:
        return []


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


def _release_all_locks():
    """Release all coordinator-held locks on shutdown."""
    try:
        db = SessionLocal()
        try:
            # Fetch locked task IDs before releasing
            locked_ids = [
                row[0] for row in
                db.query(AgentTask.id)
                .filter(AgentTask.locked_by.isnot(None))
                .all()
            ]
            updated = (
                db.query(AgentTask)
                .filter(AgentTask.locked_by.isnot(None))
                .update(
                    {"locked_by": None, "status": "queued"},
                    synchronize_session=False,
                )
            )
            db.commit()
            if updated:
                logger.info(
                    "Agent Hub: released %d lock(s) on shutdown", updated
                )
                for tid in locked_ids:
                    _publish_task(tid)
        finally:
            db.close()
    except Exception:
        logger.exception("Agent Hub: failed to release locks on shutdown")


def _safe_json_dump(obj) -> str | None:
    """JSON-serialise obj, returning None on failure."""
    if not obj:
        return None
    try:
        import json
        return json.dumps(obj, default=str)
    except Exception:
        return None


def _enrich_metadata(result) -> dict:
    """Merge adapter metadata with serialisable actions list for the event."""
    import json as _json
    meta = dict(result.metadata) if result.metadata else {}
    if result.actions:
        meta["actions"] = [
            {
                "type": a.type,
                "label": a.label,
                "command": a.command,
                "path": a.path,
            }
            for a in result.actions
        ]
        meta["actions_pending"] = True
    return meta


def _activate_chain(db, completed_task):
    """When a task completes and has a chain_task_id, set the next task to
    queued so the coordinator picks it up on the next tick."""
    next_task = db.query(AgentTask).filter(
        AgentTask.id == completed_task.chain_task_id
    ).first()
    if not next_task:
        logger.warning(
            "Agent Hub: chain target %s not found (from task %s)",
            completed_task.chain_task_id, completed_task.id,
        )
        return
    if next_task.status in ("done", "cancelled"):
        return  # already finished
    if next_task.status == "draft":
        next_task.status = "queued"
    # Inherit owner from the triggering task if next task has none
    if not next_task.current_owner and completed_task.current_owner:
        next_task.current_owner = completed_task.current_owner
    # Inherit session for grouping
    next_task.session_id = next_task.session_id or completed_task.session_id
    _record_event(
        db, next_task.id, "coordinator", "status_change",
        summary=(
            f"Auto-activated by chain from '{completed_task.title}'"
            + (f" (assigned to {next_task.current_owner})" if next_task.current_owner else "")
        ),
    )
    db.commit()
    _publish_task(next_task.id)
    logger.info(
        "Agent Hub: chain activated %s → %s",
        completed_task.id, next_task.id,
    )


def execute_pending_actions(task_id: str) -> list[dict]:
    """Read the most recent adapter message event, extract any pending actions,
    execute them, and record results as events. Called when user approves.

    Returns a list of result dicts: {success, label, output, error}.
    """
    import json as _json
    from src.adapters.base import AgentAction, execute_action
    from pathlib import Path

    results: list[dict] = []

    db = SessionLocal()
    try:
        task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
        if not task:
            return [{"success": False, "label": "Not found", "error": "Task not found"}]

        # Find the most recent adapter message event with pending actions
        event = (
            db.query(AgentEvent)
            .filter(
                AgentEvent.task_id == task_id,
                AgentEvent.event_type == "message",
                AgentEvent.actor.in_(_adapter_registry.keys()),
            )
            .order_by(AgentEvent.created_at.desc())
            .first()
        )

        if not event or not event.metadata_json:
            _record_event(db, task_id, "coordinator", "message",
                          summary="No pending actions to execute")
            db.commit()
            return results

        try:
            meta = _json.loads(event.metadata_json)
        except (_json.JSONDecodeError, TypeError):
            _record_event(db, task_id, "coordinator", "error",
                          summary="Could not parse adapter metadata for actions")
            db.commit()
            return [{"success": False, "label": "parse error", "error": "Invalid metadata JSON"}]

        raw_actions = meta.get("actions") or []
        if not raw_actions:
            _record_event(db, task_id, "coordinator", "message",
                          summary="No pending actions to execute")
            db.commit()
            return results

        base_dir = str(Path(__file__).resolve().parent.parent)  # repo root

        for i, raw in enumerate(raw_actions):
            action = AgentAction(
                type=raw.get("type", ""),
                label=raw.get("label", f"Action {i+1}"),
                command=raw.get("command", ""),
                path=raw.get("path", ""),
                workdir=raw.get("workdir"),
            )

            # Execute synchronously (called via asyncio.to_thread from route)
            result = execute_action(action, base_dir)

            results.append({
                "success": result.success,
                "label": result.label,
                "output": result.output,
                "error": result.error,
            })

            # Record as event
            event_type = "message" if result.success else "error"
            _record_event(
                db, task_id, "coordinator", event_type,
                summary=f"{'OK' if result.success else 'FAIL'}: {result.label}",
                content=result.output or result.error,
            )

            if not result.success:
                # Stop on first failure
                task.status = "blocked"
                task.last_error = result.error
                db.commit()
                return results

        # All actions succeeded — mark actions as executed
        meta["actions_pending"] = False
        event.metadata_json = _json.dumps(meta, default=str)
        db.commit()
        _publish_task(task_id)
    finally:
        db.close()

    return results
