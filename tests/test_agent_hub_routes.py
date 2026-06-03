"""Tests for Agent Hub routes — task CRUD, events, assign, approve, and status transitions.

Uses in-memory SQLite with FastAPI TestClient so no server process is needed.
"""

import sys
import types as _types
import uuid

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
if not isinstance(sqlalchemy, _types.ModuleType):
    pytest.skip("sqlalchemy is stubbed in this environment", allow_module_level=True)

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import core.database as _db
from core.database import AgentTask, AgentEvent

# Guard against module-level stubs from other test files
if type(_db.Base).__name__ == "MagicMock":
    pytest.skip("core.database is stubbed — run this file in isolation", allow_module_level=True)

from fastapi.testclient import TestClient


# ── Test app factory ──────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def client():
    """Create a FastAPI app with in-memory SQLite and the agent hub router."""
    # Use a fresh in-memory DB — StaticPool ensures all connections share the
    # same :memory: database (default pooling creates separate DBs per connection)
    engine = create_engine("sqlite:///:memory:",
                           connect_args={"check_same_thread": False},
                           poolclass=StaticPool)

    # Enable foreign keys on every connection
    @event.listens_for(engine, "connect")
    def _set_pragma(dbapi_connection, connection_record):
        import sqlite3
        if isinstance(dbapi_connection, sqlite3.Connection):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    # Create all tables (including agent_tasks and agent_events) on the test engine
    _db.Base.metadata.create_all(engine)
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # Monkey-patch so routes use our test DB
    _original_engine = _db.engine
    _original_session = _db.SessionLocal

    _db.engine = engine
    _db.SessionLocal = TestSession

    # Build a minimal FastAPI app
    from fastapi import FastAPI
    app = FastAPI()

    from routes.agent_hub_routes import setup_agent_hub_routes
    app.include_router(setup_agent_hub_routes())

    with TestClient(app) as tc:
        yield tc

    # Restore originals
    _db.engine = _original_engine
    _db.SessionLocal = _original_session


# ── Helpers ───────────────────────────────────────────────────────────────────

def _create_task(client, **kwargs) -> dict:
    body = {"title": "Test Task", **kwargs}
    res = client.post("/api/agent-hub/tasks", json=body)
    assert res.status_code == 201, res.text
    return res.json()


# ── Task CRUD ─────────────────────────────────────────────────────────────────

class TestTaskCRUD:

    def test_create_task(self, client):
        task = _create_task(client, title="My Task", objective="Do the thing")
        assert task["title"] == "My Task"
        assert task["objective"] == "Do the thing"
        assert task["status"] == "draft"
        assert task["current_owner"] is None
        assert task["id"]

    def test_create_task_defaults(self, client):
        task = _create_task(client)
        assert task["title"] == "Test Task"  # our helper default
        assert task["status"] == "draft"

    def test_create_task_queued_with_owner(self, client):
        task = _create_task(client, status="queued", current_owner="hermes")
        assert task["status"] == "queued"
        assert task["current_owner"] == "hermes"

    def test_create_task_invalid_status(self, client):
        res = client.post("/api/agent-hub/tasks", json={"status": "nonsense"})
        assert res.status_code == 400

    def test_list_tasks(self, client):
        _create_task(client, title="Task A")
        _create_task(client, title="Task B")
        res = client.get("/api/agent-hub/tasks")
        assert res.status_code == 200
        tasks = res.json()["tasks"]
        assert len(tasks) >= 2
        titles = {t["title"] for t in tasks}
        assert "Task A" in titles
        assert "Task B" in titles

    def test_list_tasks_filter_by_status(self, client):
        _create_task(client, title="Drafty", status="draft")
        _create_task(client, title="QueuedUp", status="queued")
        res = client.get("/api/agent-hub/tasks?status=draft")
        assert res.status_code == 200
        tasks = res.json()["tasks"]
        assert all(t["status"] == "draft" for t in tasks)

    def test_list_tasks_filter_by_owner_agent(self, client):
        _create_task(client, title="Hermes Job", current_owner="hermes")
        _create_task(client, title="User Job", current_owner="user")
        res = client.get("/api/agent-hub/tasks?owner=hermes")
        assert res.status_code == 200
        tasks = res.json()["tasks"]
        assert all(t["current_owner"] == "hermes" for t in tasks)

    def test_get_task(self, client):
        task = _create_task(client, title="The One")
        res = client.get(f"/api/agent-hub/tasks/{task['id']}")
        assert res.status_code == 200
        assert res.json()["title"] == "The One"

    def test_get_task_not_found(self, client):
        res = client.get(f"/api/agent-hub/tasks/{uuid.uuid4()}")
        assert res.status_code == 404

    def test_update_task_title(self, client):
        task = _create_task(client, title="Before")
        res = client.put(f"/api/agent-hub/tasks/{task['id']}", json={"title": "After"})
        assert res.status_code == 200
        assert res.json()["title"] == "After"

    def test_update_task_status_transition(self, client):
        task = _create_task(client, status="draft")
        res = client.put(f"/api/agent-hub/tasks/{task['id']}", json={"status": "queued"})
        assert res.status_code == 200
        assert res.json()["status"] == "queued"
        # Should have recorded a status-change event
        assert len(res.json()["events"]) == 1
        assert res.json()["events"][0]["event_type"] == "status_change"

    def test_update_task_invalid_transition(self, client):
        task = _create_task(client, status="done")
        res = client.put(f"/api/agent-hub/tasks/{task['id']}", json={"status": "queued"})
        assert res.status_code == 400

    def test_delete_task(self, client):
        task = _create_task(client)
        res = client.delete(f"/api/agent-hub/tasks/{task['id']}")
        assert res.status_code == 200
        assert res.json()["ok"] is True
        # Verify gone
        res2 = client.get(f"/api/agent-hub/tasks/{task['id']}")
        assert res2.status_code == 404


# ── Events ────────────────────────────────────────────────────────────────────

class TestEvents:

    def test_add_event(self, client):
        task = _create_task(client)
        res = client.post(f"/api/agent-hub/tasks/{task['id']}/events", json={
            "actor": "user",
            "event_type": "message",
            "summary": "Hello from user",
        })
        assert res.status_code == 201
        event = res.json()
        assert event["actor"] == "user"
        assert event["event_type"] == "message"
        assert event["summary"] == "Hello from user"

    def test_add_event_invalid_actor(self, client):
        task = _create_task(client)
        res = client.post(f"/api/agent-hub/tasks/{task['id']}/events", json={
            "actor": "batman",
            "event_type": "message",
        })
        assert res.status_code == 400

    def test_add_event_invalid_type(self, client):
        task = _create_task(client)
        res = client.post(f"/api/agent-hub/tasks/{task['id']}/events", json={
            "actor": "user",
            "event_type": "explosion",
        })
        assert res.status_code == 400

    def test_events_appear_in_task_timeline(self, client):
        task = _create_task(client)
        client.post(f"/api/agent-hub/tasks/{task['id']}/events", json={
            "actor": "hermes", "event_type": "message", "summary": "First",
        })
        client.post(f"/api/agent-hub/tasks/{task['id']}/events", json={
            "actor": "codex", "event_type": "message", "summary": "Second",
        })
        res = client.get(f"/api/agent-hub/tasks/{task['id']}")
        assert res.status_code == 200
        events = res.json()["events"]
        assert len(events) == 2
        summaries = {e["summary"] for e in events}
        assert "First" in summaries
        assert "Second" in summaries

    def test_events_deleted_with_task(self, client):
        task = _create_task(client)
        client.post(f"/api/agent-hub/tasks/{task['id']}/events", json={
            "actor": "user", "event_type": "message", "summary": "Gone soon",
        })
        client.delete(f"/api/agent-hub/tasks/{task['id']}")
        # Verify cascaded
        import core.database as _db
        db = _db.SessionLocal()
        try:
            events = db.query(AgentEvent).filter(AgentEvent.task_id == task["id"]).all()
            assert len(events) == 0
        finally:
            db.close()


# ── Actions ───────────────────────────────────────────────────────────────────

class TestAssign:

    def test_assign_task(self, client):
        task = _create_task(client, status="draft")
        res = client.post(f"/api/agent-hub/tasks/{task['id']}/assign",
                          json={"current_owner": "hermes"})
        assert res.status_code == 200
        assert res.json()["current_owner"] == "hermes"
        # Auto-transitions draft → queued when assigned to non-user agent
        assert res.json()["status"] == "queued"

    def test_assign_to_user_stays_draft(self, client):
        task = _create_task(client, status="draft")
        res = client.post(f"/api/agent-hub/tasks/{task['id']}/assign",
                          json={"current_owner": "user"})
        assert res.status_code == 200
        assert res.json()["status"] == "draft"

    def test_assign_invalid_owner(self, client):
        task = _create_task(client)
        res = client.post(f"/api/agent-hub/tasks/{task['id']}/assign",
                          json={"current_owner": "skynet"})
        assert res.status_code == 400


class TestApprove:

    def test_approve_task(self, client):
        task = _create_task(client, status="waiting_for_approval", approval_required=True)
        res = client.post(f"/api/agent-hub/tasks/{task['id']}/approve")
        assert res.status_code == 200
        data = res.json()
        assert data["task"]["status"] == "done"
        assert data["task"]["approval_required"] is False

    def test_approve_wrong_status(self, client):
        task = _create_task(client, status="draft")
        res = client.post(f"/api/agent-hub/tasks/{task['id']}/approve")
        assert res.status_code == 400


class TestTransition:

    def test_transition_valid(self, client):
        task = _create_task(client, status="draft")
        res = client.post(f"/api/agent-hub/tasks/{task['id']}/transition",
                          json={"status": "queued"})
        assert res.status_code == 200
        assert res.json()["status"] == "queued"

    def test_transition_invalid(self, client):
        task = _create_task(client, status="done")
        res = client.post(f"/api/agent-hub/tasks/{task['id']}/transition",
                          json={"status": "queued"})
        assert res.status_code == 400

    def test_force_cancel_releases_lock(self, client):
        from datetime import datetime
        import core.database as _db
        # Create a locked running task
        task = _create_task(client, status="running")
        db = _db.SessionLocal()
        try:
            t = db.query(AgentTask).filter(AgentTask.id == task["id"]).first()
            t.locked_by = "hermes"
            t.locked_at = datetime.utcnow()
            db.commit()
        finally:
            db.close()

        # Without force_cancel — should fail
        res1 = client.post(f"/api/agent-hub/tasks/{task['id']}/transition",
                           json={"status": "cancelled"})
        assert res1.status_code == 409

        # With force_cancel — should succeed
        res2 = client.post(f"/api/agent-hub/tasks/{task['id']}/transition",
                           json={"status": "cancelled", "force_cancel": True})
        assert res2.status_code == 200
        assert res2.json()["status"] == "cancelled"
        assert res2.json()["locked_by"] is None


# ── Keyword search ─────────────────────────────────────────────────────────────

class TestKeywordSearch:

    def test_search_matches_title(self, client):
        _create_task(client, title="Fix login bug")
        _create_task(client, title="Deploy staging")
        _create_task(client, title="Update login page")
        res = client.get("/api/agent-hub/tasks?q=login")
        assert res.status_code == 200
        tasks = res.json()["tasks"]
        assert len(tasks) == 2
        titles = {t["title"] for t in tasks}
        assert titles == {"Fix login bug", "Update login page"}

    def test_search_matches_objective(self, client):
        _create_task(client, title="Task A", objective="fix the zephyr flow")
        _create_task(client, title="Task B", objective="deploy config")
        res = client.get("/api/agent-hub/tasks?q=zephyr")
        assert res.status_code == 200
        tasks = res.json()["tasks"]
        assert len(tasks) == 1
        assert tasks[0]["title"] == "Task A"

    def test_search_no_match(self, client):
        _create_task(client, title="Fix login")
        res = client.get("/api/agent-hub/tasks?q=oojamaflip")
        assert res.status_code == 200
        assert res.json()["tasks"] == []

    def test_search_combines_with_status_filter(self, client):
        _create_task(client, title="Fix login bug", status="queued")
        _create_task(client, title="Login refactor", status="done")
        res = client.get("/api/agent-hub/tasks?q=login&status=queued")
        assert res.status_code == 200
        tasks = res.json()["tasks"]
        assert len(tasks) == 1
        assert tasks[0]["title"] == "Fix login bug"


# ── Export timeline ────────────────────────────────────────────────────────────

class TestExportTimeline:

    def test_export_markdown(self, client):
        task = _create_task(client, title="Export test", objective="Test the export")
        # Add an event
        client.post(f"/api/agent-hub/tasks/{task['id']}/events", json={
            "actor": "hermes",
            "event_type": "message",
            "summary": "Working on it",
            "content": "some output",
        })
        res = client.get(f"/api/agent-hub/tasks/{task['id']}/export")
        assert res.status_code == 200
        assert res.headers["content-type"].startswith("text/markdown")
        md = res.text
        assert "Export test" in md
        assert "Test the export" in md
        assert "Working on it" in md
        assert "some output" in md
        assert "hermes" in md
        assert "Timeline" in md

    def test_export_nonexistent_task(self, client):
        res = client.get("/api/agent-hub/tasks/nonexistent-id/export")
        assert res.status_code == 404

    def test_export_empty_timeline(self, client):
        task = _create_task(client, title="No events", objective="Nothing happened")
        res = client.get(f"/api/agent-hub/tasks/{task['id']}/export")
        assert res.status_code == 200
        md = res.text
        assert "No events" in md
        assert "No events recorded" in md


# ── Coordinator status ────────────────────────────────────────────────────────

class TestCoordinatorStatus:

    def test_status_returns_placeholder(self, client):
        res = client.get("/api/agent-hub/status")
        assert res.status_code == 200
        data = res.json()
        # With no coordinator running in this test app, status reports idle
        assert "running" in data


# ── Sandbox mode ──────────────────────────────────────────────────────────────

class TestSandboxMode:

    def test_default_sandbox_is_workspace_write(self, client):
        task = _create_task(client, title="Sandbox default")
        assert task["sandbox_mode"] == "workspace-write"

    def test_create_with_explicit_sandbox(self, client):
        task = _create_task(client, title="Read-only task",
                            sandbox_mode="read-only")
        assert task["sandbox_mode"] == "read-only"

    def test_create_with_danger_full_access(self, client):
        task = _create_task(client, title="Danger task",
                            sandbox_mode="danger-full-access")
        assert task["sandbox_mode"] == "danger-full-access"

    def test_reject_invalid_sandbox_mode_on_create(self, client):
        res = client.post("/api/agent-hub/tasks", json={
            "title": "Bad sandbox",
            "sandbox_mode": "bananas",
        })
        assert res.status_code == 400
        assert "sandbox_mode" in res.text.lower()

    def test_reject_invalid_sandbox_mode_on_update(self, client):
        task = _create_task(client, title="Update sandbox test")
        res = client.put(f"/api/agent-hub/tasks/{task['id']}", json={
            "sandbox_mode": "bananas",
        })
        assert res.status_code == 400
        assert "sandbox_mode" in res.text.lower()

    def test_update_sandbox_mode(self, client):
        task = _create_task(client, title="Will change sandbox")
        assert task["sandbox_mode"] == "workspace-write"
        res = client.put(f"/api/agent-hub/tasks/{task['id']}", json={
            "sandbox_mode": "danger-full-access",
        })
        assert res.status_code == 200
        updated = res.json()
        assert updated["sandbox_mode"] == "danger-full-access"

    def test_sandbox_appears_in_list(self, client):
        _create_task(client, title="List sandbox test",
                     sandbox_mode="read-only")
        res = client.get("/api/agent-hub/tasks")
        tasks = res.json()["tasks"]
        assert any(t["title"] == "List sandbox test"
                   and t["sandbox_mode"] == "read-only"
                   for t in tasks)
