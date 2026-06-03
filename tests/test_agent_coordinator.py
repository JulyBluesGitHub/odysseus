"""Tests for the Agent Hub coordinator — claim, dispatch, transition, error handling.

Uses in-memory SQLite with the real coordinator and mock adapter.
"""

import sys
import types as _types

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
if not isinstance(sqlalchemy, _types.ModuleType):
    pytest.skip("sqlalchemy is stubbed in this environment", allow_module_level=True)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import core.database as _db
from core.database import AgentTask, AgentEvent
import src.agent_coordinator as _ac

if type(_db.Base).__name__ == "MagicMock":
    pytest.skip("core.database is stubbed — run this file in isolation", allow_module_level=True)

import asyncio
import uuid
from datetime import datetime, timezone


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_engine():
    """Create an in-memory SQLite engine with shared connection pool."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _db.Base.metadata.create_all(engine)
    return engine


def _make_session(engine):
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)()


def _create_task(db, **kwargs) -> AgentTask:
    defaults = {
        "id": str(uuid.uuid4()),
        "title": "Test Task",
        "status": "draft",
    }
    defaults.update(kwargs)
    task = AgentTask(**defaults)
    db.add(task)
    db.commit()
    db.refresh(task)
    return task


def _patch_session(monkeypatch, engine):
    """Monkey-patch SessionLocal in both core.database and agent_coordinator,
    since the coordinator imports it at module load time."""
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    monkeypatch.setattr(_db, "SessionLocal", TestSession)
    monkeypatch.setattr(_ac, "SessionLocal", TestSession)
    return TestSession


def _setup_coordinator_registry():
    """Clear and return the adapter registry for clean test state."""
    _ac._adapter_registry.clear()
    return _ac._adapter_registry


# ── Transition validation ─────────────────────────────────────────────────────

class TestTransitionValidation:

    def test_valid_transition(self):
        assert _ac._coordinator_can_transition("queued", "running") is True
        assert _ac._coordinator_can_transition("running", "done") is True
        assert _ac._coordinator_can_transition("running", "waiting_for_approval") is True
        assert _ac._coordinator_can_transition("running", "blocked") is True
        assert _ac._coordinator_can_transition("running", "queued") is True

    def test_invalid_transition(self):
        assert _ac._coordinator_can_transition("draft", "running") is False
        assert _ac._coordinator_can_transition("done", "queued") is False
        assert _ac._coordinator_can_transition("cancelled", "queued") is False
        assert _ac._coordinator_can_transition("queued", "done") is False

    def test_unknown_status(self):
        assert _ac._coordinator_can_transition("nonsense", "done") is False


# ── Coordinator tick (integration) ─────────────────────────────────────────────

class TestCoordinatorTick:
    """Test a single coordinator tick against an in-memory database."""

    def test_tick_claims_and_processes_queued_task(self, monkeypatch):
        """A queued task with a registered adapter gets claimed and processed."""
        engine = _make_engine()
        _patch_session(monkeypatch, engine)

        db = _make_session(engine)
        task = _create_task(db, status="queued", current_owner="mock",
                            title="Process Me", objective="Do something")
        db.close()

        from src.adapters.mock import MockAdapter
        _setup_coordinator_registry()
        _ac.register_adapter("mock", MockAdapter())

        asyncio.run(_ac._tick())

        db2 = _make_session(engine)
        try:
            t = db2.query(AgentTask).filter(AgentTask.id == task.id).first()
            assert t is not None
            assert t.status == "done"
            assert t.locked_by is None
            assert t.locked_at is not None  # kept for timing
            assert t.current_owner == "user"

            events = (
                db2.query(AgentEvent)
                .filter(AgentEvent.task_id == task.id)
                .order_by(AgentEvent.created_at)
                .all()
            )
            event_types = [e.event_type for e in events]
            assert "lock" in event_types
            assert "message" in event_types
            assert "status_change" in event_types
        finally:
            db2.close()

    def test_tick_ignores_non_queued_task(self, monkeypatch):
        """Tasks not in 'queued' status are skipped."""
        engine = _make_engine()
        _patch_session(monkeypatch, engine)

        db = _make_session(engine)
        _create_task(db, status="draft", current_owner="mock", title="Drafty")
        _create_task(db, status="done", current_owner="mock", title="Donezo")
        db.close()

        from src.adapters.mock import MockAdapter
        _setup_coordinator_registry()
        _ac.register_adapter("mock", MockAdapter())

        asyncio.run(_ac._tick())

        db2 = _make_session(engine)
        try:
            tasks = db2.query(AgentTask).all()
            assert all(t.status in ("draft", "done") for t in tasks)
        finally:
            db2.close()

    def test_tick_ignores_unregistered_owner(self, monkeypatch):
        """Tasks assigned to an unregistered owner are skipped."""
        engine = _make_engine()
        _patch_session(monkeypatch, engine)

        db = _make_session(engine)
        _create_task(db, status="queued", current_owner="skynet", title="Unregistered")
        db.close()

        from src.adapters.mock import MockAdapter
        _setup_coordinator_registry()
        _ac.register_adapter("mock", MockAdapter())

        asyncio.run(_ac._tick())

        db2 = _make_session(engine)
        try:
            t = db2.query(AgentTask).first()
            assert t.status == "queued"
            assert t.locked_by is None
        finally:
            db2.close()

    def test_tick_handles_adapter_crash(self, monkeypatch):
        """A crashing adapter records an error event and re-queues."""
        engine = _make_engine()
        _patch_session(monkeypatch, engine)

        db = _make_session(engine)
        task = _create_task(db, status="queued", current_owner="mock",
                            title="Crash Test")
        db.close()

        class BrokenAdapter:
            async def probe(self):
                from src.adapters.base import AdapterProbe
                return AdapterProbe(available=True)
            async def run(self, task, events):
                raise RuntimeError("simulated crash")

        _setup_coordinator_registry()
        _ac.register_adapter("mock", BrokenAdapter())

        asyncio.run(_ac._tick())

        db2 = _make_session(engine)
        try:
            t = db2.query(AgentTask).filter(AgentTask.id == task.id).first()
            assert t.status == "queued"
            assert t.attempt_count == 1
            events = db2.query(AgentEvent).filter(
                AgentEvent.task_id == task.id,
                AgentEvent.event_type == "error",
            ).all()
            assert len(events) >= 1
            assert "simulated crash" in events[0].content
        finally:
            db2.close()

    def test_tick_blocks_after_max_attempts(self, monkeypatch):
        """After MAX_ATTEMPTS failures, the task is blocked."""
        engine = _make_engine()
        _patch_session(monkeypatch, engine)

        db = _make_session(engine)
        task = _create_task(db, status="queued", current_owner="mock",
                            title="Persistent Failure", attempt_count=3)
        db.close()

        class BrokenAdapter:
            async def probe(self):
                from src.adapters.base import AdapterProbe
                return AdapterProbe(available=True)
            async def run(self, task, events):
                raise RuntimeError("still broken")

        _setup_coordinator_registry()
        _ac.register_adapter("mock", BrokenAdapter())

        asyncio.run(_ac._tick())

        db2 = _make_session(engine)
        try:
            t = db2.query(AgentTask).filter(AgentTask.id == task.id).first()
            assert t.status == "blocked"
            assert "still broken" in (t.last_error or "")
        finally:
            db2.close()

    def test_tick_applies_needs_approval(self, monkeypatch):
        """Adapter result with needs_approval=True sets waiting_for_approval."""
        engine = _make_engine()
        _patch_session(monkeypatch, engine)

        db = _make_session(engine)
        task = _create_task(db, status="queued", current_owner="mock",
                            title="Approval Please")
        db.close()

        class ApprovingAdapter:
            async def probe(self):
                from src.adapters.base import AdapterProbe
                return AdapterProbe(available=True)
            async def run(self, task, events):
                from src.adapters.base import AgentAdapterResult
                return AgentAdapterResult(
                    summary="Needs your OK",
                    content="Please approve this action.",
                    proposed_status="done",
                    needs_approval=True,
                )

        _setup_coordinator_registry()
        _ac.register_adapter("mock", ApprovingAdapter())

        asyncio.run(_ac._tick())

        db2 = _make_session(engine)
        try:
            t = db2.query(AgentTask).filter(AgentTask.id == task.id).first()
            assert t.status == "waiting_for_approval"
            assert t.approval_required is True
        finally:
            db2.close()

    def test_tick_rejects_invalid_proposed_transition(self, monkeypatch):
        """Coordinator ignores adapter-proposed transitions that aren't allowed."""
        engine = _make_engine()
        _patch_session(monkeypatch, engine)

        db = _make_session(engine)
        task = _create_task(db, status="queued", current_owner="mock",
                            title="Bad Transition")
        db.close()

        class BadTransitionAdapter:
            async def probe(self):
                from src.adapters.base import AdapterProbe
                return AdapterProbe(available=True)
            async def run(self, task, events):
                from src.adapters.base import AgentAdapterResult
                return AgentAdapterResult(
                    summary="Trying to skip to cancelled",
                    content="",
                    proposed_status="cancelled",
                )

        _setup_coordinator_registry()
        _ac.register_adapter("mock", BadTransitionAdapter())

        asyncio.run(_ac._tick())

        db2 = _make_session(engine)
        try:
            t = db2.query(AgentTask).filter(AgentTask.id == task.id).first()
            assert t.status == "running"
        finally:
            db2.close()


# ── Lock release on shutdown ──────────────────────────────────────────────────

class TestLockRelease:

    def test_release_all_locks_on_shutdown(self, monkeypatch):
        """_release_all_locks clears locked_by on all tasks."""
        engine = _make_engine()
        _patch_session(monkeypatch, engine)

        db = _make_session(engine)
        t1 = _create_task(db, status="running", current_owner="mock",
                          locked_by="mock",
                          locked_at=datetime.now(timezone.utc))
        t2 = _create_task(db, status="running", current_owner="hermes",
                          locked_by="hermes",
                          locked_at=datetime.now(timezone.utc))
        tid1, tid2 = t1.id, t2.id
        db.close()

        _ac._release_all_locks()

        db2 = _make_session(engine)
        try:
            for tid in (tid1, tid2):
                t = db2.query(AgentTask).filter(AgentTask.id == tid).first()
                assert t is not None
                assert t.locked_by is None
                assert t.status == "queued"
        finally:
            db2.close()
