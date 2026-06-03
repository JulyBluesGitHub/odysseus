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
        if not action.role or action.role not in (VALID_ROLES if False else VALID_ROLES):  # always True
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
    }


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
    """Single coordinator tick — claim one task and dispatch it.

    Tasks can target a role (diagnoser/implementer/verifier) or specify an
    adapter directly via current_owner. For role-based tasks, the coordinator
    resolves role → adapter via the RoleBinding table at claim time and records
    the resolution in the timeline.
    """
    global _tasks_processed

    db = SessionLocal()
    try:
        # Find the oldest queued, unlocked task that is either:
        # - directly assigned to a registered adapter, OR
        # - assigned to a role (will be resolved below)
        task = (
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
            .first()
        )
        if not task:
            return

        # Resolve role → adapter if needed
        if task.role:
            if task.role not in VALID_ROLES:
                task.status = "blocked"
                task.last_error = f"Invalid role: {task.role}"
                _record_event(
                    db, task.id, "coordinator", "error",
                    summary=f"Invalid role '{task.role}' — valid roles: {', '.join(sorted(VALID_ROLES))}",
                )
                db.commit()
                _publish_task(task.id)
                return

            resolved = _resolve_role(task.role, task.owner)
            if not resolved:
                task.status = "blocked"
                task.last_error = f"No adapter bound to role '{task.role}'"
                _record_event(
                    db, task.id, "coordinator", "error",
                    summary=f"No adapter binding for role '{task.role}'",
                )
                db.commit()
                _publish_task(task.id)
                return

            old_owner = task.current_owner
            task.current_owner = resolved
            _record_event(
                db, task.id, "coordinator", "status_change",
                summary=(
                    f"Resolved role {task.role} to adapter {resolved}"
                    + (f" (was directly assigned to {old_owner})" if old_owner and old_owner != resolved else "")
                ),
            )

        # Check dependencies — skip if any aren't done
        if task.depends_on:
            unmet = _get_unmet_dependencies(task, db)
            if unmet:
                # Task stays queued — it's waiting on dependencies
                db.commit()  # save any resolution events above
                _publish_task(task.id)
                return

        owner = task.current_owner
        if owner not in _adapter_registry:
            logger.warning(
                "Agent Hub: resolved adapter '%s' not in registry for task %s",
                owner, task.id,
            )
            return

        # Claim the task atomically
        task.locked_by = owner
        task.locked_at = datetime.now(timezone.utc)
        task.attempt_count = (task.attempt_count or 0) + 1
        task.status = "running"
        _record_event(
            db, task.id, "coordinator", "lock",
            summary=f"Claimed by {owner} (attempt {task.attempt_count})",
        )
        db.commit()
        db.refresh(task)
        _publish_task(task.id)
    finally:
        db.close()

    # Dispatch outside the claim transaction so slow adapters don't hold locks
    # on the connection pool. Re-open a session for the result write.
    adapter = _adapter_registry[owner]
    events = _load_events(task.id)

    try:
        result = await adapter.run(task, events)
    except Exception as exc:
        result = None
        error_msg = f"{type(exc).__name__}: {exc}"

    # Write result
    db2 = SessionLocal()
    try:
        task = db2.query(AgentTask).filter(AgentTask.id == task.id).first()
        if not task:
            return

        if result is None:
            # Adapter crashed
            _record_event(
                db2, task.id, "coordinator", "error",
                summary=f"Adapter '{owner}' failed: {error_msg}",
                content=error_msg,
            )
            if task.attempt_count >= MAX_ATTEMPTS:
                task.status = "blocked"
                task.last_error = error_msg
                _record_event(
                    db2, task.id, "coordinator", "status_change",
                    summary=f"Blocked after {task.attempt_count} failed attempts",
                )
            else:
                task.status = "queued"
        else:
            # Adapter succeeded — record the output
            _record_event(
                db2, task.id, owner, "message",
                summary=result.summary,
                content=result.content,
                metadata_json=_safe_json_dump(_enrich_metadata(result)),
            )
            # Process create_task actions immediately (no approval needed)
            _execute_create_task_actions(db2, task, result.actions)
            # Apply proposed transitions
            if result.needs_approval:
                # Approval takes precedence over any status proposal.
                # If the adapter says "needs approval," the task waits for the
                # user regardless of what status it proposed.
                task.approval_required = True
                if task.status not in ("done", "cancelled"):
                    task.status = "waiting_for_approval"
                    _record_event(
                        db2, task.id, "coordinator", "status_change",
                        summary="Waiting for user approval",
                    )
            elif result.proposed_status:
                proposed = result.proposed_status
                if _coordinator_can_transition(task.status, proposed):
                    old_status = task.status
                    task.status = proposed
                    _record_event(
                        db2, task.id, "coordinator", "status_change",
                        summary=f"Status: {old_status} → {proposed}",
                    )
                else:
                    logger.warning(
                        "Agent Hub: adapter '%s' proposed invalid transition "
                        "'%s' → '%s' for task %s",
                        owner, task.status, proposed, task.id,
                    )

            # Apply proposed owner
            if result.proposed_owner:
                task.current_owner = result.proposed_owner

        # Release lock (keep locked_at for timing — poll only checks locked_by)
        task.locked_by = None
        db2.commit()
        _publish_task(task.id)
        _tasks_processed += 1

        # Chain: if task completed and has a chain_task_id, auto-create the next one
        if task.status == "done" and task.chain_task_id:
            _activate_chain(db2, task)
    except Exception:
        logger.exception("Agent Hub: failed to write coordinator result")
        db2.rollback()
    finally:
        db2.close()


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
