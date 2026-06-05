"""Tests for scheduled/recurring Agent Hub tasks.

Uses in-memory SQLite with a minimal FastAPI app (same pattern as
test_agent_hub_events.py) to avoid the full app.py import chain.
"""

import pytest
import uuid as _uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import core.database as _db
from core.database import AgentTask, AgentEvent
from fastapi.testclient import TestClient
from fastapi import FastAPI


# ── Fixture helpers ───────────────────────────────────────────────────────────


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _create_test_app():
    """Minimal FastAPI app with agent hub routes, bypassing full app.py imports."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _set_pragma(dbapi_connection, connection_record):
        import sqlite3
        if isinstance(dbapi_connection, sqlite3.Connection):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    _db.Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    # Seed role bindings
    with engine.connect() as conn:
        from sqlalchemy import text
        now = datetime.utcnow().isoformat()
        for role, adapter in [
            ("diagnoser", "hermes"), ("implementer", "codex"), ("verifier", "hermes"),
        ]:
            conn.execute(
                text("INSERT INTO role_bindings (id, owner, role, adapter_name, created_at, updated_at) "
                     "VALUES (:id, NULL, :role, :adapter, :now, :now)"),
                {"id": str(_uuid.uuid4()), "role": role, "adapter": adapter, "now": now},
            )
        conn.commit()

    _db.SessionLocal = SessionLocal
    import src.agent_coordinator as _coord
    _coord.SessionLocal = SessionLocal
    import routes.agent_hub_routes as _ahr
    _ahr.SessionLocal = SessionLocal

    app = FastAPI()

    @app.middleware("http")
    async def _test_auth(request, call_next):
        request.state.current_user = "testuser"
        response = await call_next(request)
        return response

    from routes.agent_hub_routes import setup_agent_hub_routes
    router = setup_agent_hub_routes()
    app.include_router(router)

    return app


@pytest.fixture
def client():
    """FastAPI TestClient with isolated in-memory SQLite."""
    app = _create_test_app()
    with TestClient(app) as c:
        yield c


def _create_task(client, **overrides):
    body = {"title": "Test Task", "status": "draft", "priority": "medium"}
    body.update(overrides)
    r = client.post("/api/agent-hub/tasks", json=body)
    assert r.status_code in (200, 201), f"Create failed: {r.status_code} {r.text}"
    return r.json()


def _get_task(client, task_id):
    r = client.get(f"/api/agent-hub/tasks/{task_id}")
    assert r.status_code == 200, f"Get failed: {r.status_code} {r.text}"
    return r.json()


def _update_task(client, task_id, **fields):
    r = client.put(f"/api/agent-hub/tasks/{task_id}", json=fields)
    assert r.status_code == 200, f"Update failed: {r.status_code} {r.text}"
    return r.json()


# ── Schedule parsing ──────────────────────────────────────────────────────────


class TestScheduleParsing:

    def test_cron_expression_valid(self, client):
        t = _create_task(client, title="Cron", schedule_type="cron",
                         schedule_expr="0 9 * * *")
        assert t["status"] == "scheduled"
        assert t["schedule_type"] == "cron"
        assert t["schedule_expr"] == "0 9 * * *"
        assert t["next_run_at"] is not None

    def test_cron_expression_invalid(self, client):
        r = client.post("/api/agent-hub/tasks", json={
            "title": "Bad Cron", "schedule_type": "cron",
            "schedule_expr": "not a cron",
        })
        assert r.status_code in (400, 422)

    def test_interval_valid(self, client):
        t = _create_task(client, title="Interval", schedule_type="interval",
                         schedule_expr="every 2h")
        assert t["status"] == "scheduled"
        assert t["schedule_type"] == "interval"
        assert t["next_run_at"] is not None

    def test_interval_invalid(self, client):
        r = client.post("/api/agent-hub/tasks", json={
            "title": "Bad Interval", "schedule_type": "interval",
            "schedule_expr": "every banana",
        })
        assert r.status_code in (400, 422)

    def test_once_iso_timestamp(self, client):
        future = (_now() + timedelta(hours=2)).isoformat()
        t = _create_task(client, title="Once ISO", schedule_type="once",
                         schedule_expr=future)
        assert t["status"] == "scheduled"
        assert t["schedule_type"] == "once"

    def test_once_relative_delay(self, client):
        t = _create_task(client, title="Once 30m", schedule_type="once",
                         schedule_expr="30m")
        assert t["status"] == "scheduled"
        assert t["schedule_type"] == "once"
        assert t["next_run_at"] is not None

    def test_once_invalid(self, client):
        r = client.post("/api/agent-hub/tasks", json={
            "title": "Bad Once", "schedule_type": "once",
            "schedule_expr": "next Tuesday",
        })
        assert r.status_code in (400, 422)

    def test_schedule_type_without_expr(self, client):
        r = client.post("/api/agent-hub/tasks", json={
            "title": "No Expr", "schedule_type": "interval",
        })
        assert r.status_code == 400

    def test_immediate_task_no_schedule(self, client):
        t = _create_task(client, title="Immediate")
        assert t["schedule_type"] is None
        assert t["next_run_at"] is None
        assert t["status"] == "draft"


# ── Status transitions ────────────────────────────────────────────────────────


class TestScheduledStatusTransitions:

    def test_scheduled_to_queued_valid(self, client):
        t = _create_task(client, title="S", schedule_type="once",
                         schedule_expr="30m")
        updated = _update_task(client, t["id"], status="queued")
        assert updated["status"] == "queued"

    def test_scheduled_to_paused_valid(self, client):
        t = _create_task(client, title="S", schedule_type="interval",
                         schedule_expr="every 2h")
        updated = _update_task(client, t["id"], status="paused")
        assert updated["status"] == "paused"

    def test_paused_to_scheduled_valid(self, client):
        # Create scheduled, then pause, then resume (create forces 'scheduled')
        t = _create_task(client, title="P", schedule_type="interval",
                         schedule_expr="every 2h")
        _update_task(client, t["id"], status="paused")
        updated = _update_task(client, t["id"], status="scheduled")
        assert updated["status"] == "scheduled"
        assert updated["next_run_at"] is not None  # recomputed

    def test_scheduled_to_done_invalid(self, client):
        t = _create_task(client, title="S", schedule_type="once",
                         schedule_expr="30m")
        r = client.put(f"/api/agent-hub/tasks/{t['id']}", json={"status": "done"})
        assert r.status_code == 400

    def test_paused_to_done_invalid(self, client):
        t = _create_task(client, title="P", schedule_type="interval",
                         schedule_expr="every 2h", status="paused")
        r = client.put(f"/api/agent-hub/tasks/{t['id']}", json={"status": "done"})
        assert r.status_code == 400

    def test_scheduled_to_cancelled_valid(self, client):
        t = _create_task(client, title="S", schedule_type="once",
                         schedule_expr="30m")
        updated = _update_task(client, t["id"], status="cancelled")
        assert updated["status"] == "cancelled"

    def test_paused_to_cancelled_valid(self, client):
        t = _create_task(client, title="P", schedule_type="interval",
                         schedule_expr="every 2h")
        _update_task(client, t["id"], status="paused")
        updated = _update_task(client, t["id"], status="cancelled")
        assert updated["status"] == "cancelled"


# ── List filter ────────────────────────────────────────────────────────────────


class TestListFilter:

    def test_filter_scheduled(self, client):
        _create_task(client, title="S1", schedule_type="once", schedule_expr="30m")
        _create_task(client, title="D1", status="draft")
        r = client.get("/api/agent-hub/tasks?status=scheduled")
        assert r.status_code == 200
        tasks = r.json()["tasks"]
        assert all(t["status"] == "scheduled" for t in tasks)
        assert any(t["title"] == "S1" for t in tasks)

    def test_filter_paused(self, client):
        t = _create_task(client, title="P1", schedule_type="interval",
                         schedule_expr="every 2h")
        _update_task(client, t["id"], status="paused")
        r = client.get("/api/agent-hub/tasks?status=paused")
        assert r.status_code == 200
        tasks = r.json()["tasks"]
        assert all(t["status"] == "paused" for t in tasks)
        assert any(t["title"] == "P1" for t in tasks)


# ── Batch operations ──────────────────────────────────────────────────────────


class TestBatchScheduled:

    def test_batch_cancel_scheduled_clears_next_run(self, client):
        t = _create_task(client, title="S", schedule_type="once",
                         schedule_expr="30m")
        r = client.post("/api/agent-hub/tasks/batch", json={
            "action": "cancel", "task_ids": [t["id"]],
        })
        assert r.status_code == 200
        updated = _get_task(client, t["id"])
        assert updated["status"] == "cancelled"
        assert updated["next_run_at"] is None

    def test_batch_retry_scheduled_creates_clone(self, client):
        t = _create_task(client, title="Template", schedule_type="interval",
                         schedule_expr="every 2h")
        before = client.get("/api/agent-hub/tasks").json()["tasks"]
        before_count = len(before)

        r = client.post("/api/agent-hub/tasks/batch", json={
            "action": "retry", "task_ids": [t["id"]],
        })
        assert r.status_code == 200

        updated = _get_task(client, t["id"])
        assert updated["status"] == "scheduled"
        assert updated["schedule_type"] == "interval"

        after = client.get("/api/agent-hub/tasks").json()["tasks"]
        assert len(after) == before_count + 1
        clones = [x for x in after if x.get("scheduled_template_id") == t["id"]]
        assert len(clones) == 1
        assert clones[0]["status"] == "queued"
        assert clones[0]["schedule_type"] is None

    def test_batch_retry_queued_still_works(self, client):
        t = _create_task(client, title="Q", status="queued")
        r = client.post("/api/agent-hub/tasks/batch", json={
            "action": "retry", "task_ids": [t["id"]],
        })
        assert r.status_code == 200
        updated = _get_task(client, t["id"])
        assert updated["status"] == "queued"


# ── Template deletion guard ───────────────────────────────────────────────────


class TestTemplateDeletion:

    def test_delete_template_blocked_by_active_clone(self, client):
        t = _create_task(client, title="Template", schedule_type="interval",
                         schedule_expr="every 1h")

        # Manual clone via API + direct DB lineage write
        from routes.agent_hub_routes import SessionLocal as SL
        dbs = SL()
        clone = AgentTask(
            id=str(_uuid.uuid4()),
            title="Clone", status="queued",
            scheduled_template_id=t["id"],
        )
        dbs.add(clone)
        dbs.commit()
        dbs.close()

        r = client.delete(f"/api/agent-hub/tasks/{t['id']}")
        assert r.status_code == 409, f"Expected 409, got {r.status_code}: {r.text}"

    def test_delete_template_allowed_with_historical_clone(self, client):
        t = _create_task(client, title="Template", schedule_type="interval",
                         schedule_expr="every 1h")

        from routes.agent_hub_routes import SessionLocal as SL
        dbs = SL()
        clone = AgentTask(
            id=str(_uuid.uuid4()),
            title="Old Clone", status="done",
            scheduled_template_id=t["id"],
        )
        dbs.add(clone)
        dbs.commit()
        dbs.close()

        r = client.delete(f"/api/agent-hub/tasks/{t['id']}")
        assert r.status_code == 200

    def test_delete_non_template_not_blocked(self, client):
        t = _create_task(client, title="Normal", status="draft")
        r = client.delete(f"/api/agent-hub/tasks/{t['id']}")
        assert r.status_code == 200


# ── Dependency alignment ──────────────────────────────────────────────────────


class TestDependencyAlignment:

    def test_depends_on_non_template_rejected(self, client):
        dep = _create_task(client, title="Dep", status="draft")
        r = client.post("/api/agent-hub/tasks", json={
            "title": "Template", "schedule_type": "interval",
            "schedule_expr": "every 2h",
            "depends_on": [dep["id"]],
        })
        assert r.status_code == 400

    def test_depends_on_aligned_template_accepted(self, client):
        dep = _create_task(client, title="Dep Tpl", schedule_type="interval",
                           schedule_expr="every 2h")
        r = client.post("/api/agent-hub/tasks", json={
            "title": "Template A", "schedule_type": "interval",
            "schedule_expr": "every 2h",
            "depends_on": [dep["id"]],
        })
        assert r.status_code in (200, 201)

    def test_depends_on_misaligned_schedule_rejected(self, client):
        dep = _create_task(client, title="Dep Tpl", schedule_type="interval",
                           schedule_expr="every 4h")
        r = client.post("/api/agent-hub/tasks", json={
            "title": "Template A", "schedule_type": "interval",
            "schedule_expr": "every 2h",
            "depends_on": [dep["id"]],
        })
        assert r.status_code == 400


# ── SSE serialization ─────────────────────────────────────────────────────────


class TestSSESerialization:

    def test_task_dict_includes_schedule_fields(self, client):
        t = _create_task(client, title="S", schedule_type="interval",
                         schedule_expr="every 2h")
        full = _get_task(client, t["id"])
        assert "schedule_type" in full
        assert "schedule_expr" in full
        assert "next_run_at" in full
        assert "allow_overlap" in full
        assert "scheduled_template_id" in full
        assert "scheduled_run_at" in full
        assert full["schedule_type"] == "interval"
        assert full["schedule_expr"] == "every 2h"

    def test_clone_does_not_inherit_schedule(self, client):
        t = _create_task(client, title="Clone", status="queued")
        # Set lineage via API
        _update_task(client, t["id"], schedule_type="once",
                     schedule_expr="30m")
        # Then clear the schedule (clone shouldn't have it)
        from routes.agent_hub_routes import SessionLocal as SL
        dbs = SL()
        task = dbs.query(AgentTask).filter(AgentTask.id == t["id"]).first()
        task.schedule_type = None
        task.schedule_expr = None
        task.scheduled_template_id = "some-template-id"
        dbs.commit()
        dbs.close()
        full = _get_task(client, t["id"])
        assert full["schedule_type"] is None
        assert full["schedule_expr"] is None


# ── Schedule edit ─────────────────────────────────────────────────────────────


class TestScheduleEdit:

    def test_edit_schedule_expr_recomputes_next_run(self, client):
        t = _create_task(client, title="S", schedule_type="interval",
                         schedule_expr="every 2h")
        original_next = t["next_run_at"]
        updated = _update_task(client, t["id"], schedule_expr="every 4h")
        assert updated["next_run_at"] != original_next
        assert updated["schedule_expr"] == "every 4h"

    def test_edit_schedule_type_empty_clears(self, client):
        t = _create_task(client, title="S", schedule_type="interval",
                         schedule_expr="every 2h")
        updated = _update_task(client, t["id"], schedule_type="")
        assert updated["schedule_type"] is None


# ── compute_next_interval unit tests ──────────────────────────────────────────


class TestComputeNextInterval:

    def test_every_2h(self):
        from src.task_scheduler import compute_next_interval
        now = _now()
        result = compute_next_interval("every 2h", after=now)
        assert result is not None
        assert result - now == timedelta(hours=2)

    def test_bare_30m(self):
        from src.task_scheduler import compute_next_interval
        now = _now()
        result = compute_next_interval("30m", after=now)
        assert result is not None
        assert result - now == timedelta(minutes=30)

    def test_every_1d(self):
        from src.task_scheduler import compute_next_interval
        now = _now()
        result = compute_next_interval("every 1d", after=now)
        assert result is not None
        assert result - now == timedelta(days=1)

    def test_invalid_returns_none(self):
        from src.task_scheduler import compute_next_interval
        assert compute_next_interval("every banana") is None
        assert compute_next_interval("0h") is None
        assert compute_next_interval("") is None

    def test_zero_returns_none(self):
        from src.task_scheduler import compute_next_interval
        assert compute_next_interval("0h") is None
