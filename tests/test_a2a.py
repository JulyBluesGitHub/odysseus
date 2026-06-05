"""Tests for the Agent Hub A2A compatibility layer."""

import types as _types

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
if not isinstance(sqlalchemy, _types.ModuleType):
    pytest.skip("sqlalchemy is stubbed in this environment", allow_module_level=True)

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import core.database as _db
from core.database import AgentEvent, AgentInstance, AgentTask
from src.adapters.base import AgentAdapterResult

if type(_db.Base).__name__ == "MagicMock":
    pytest.skip("core.database is stubbed", allow_module_level=True)


@pytest.fixture()
def client(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _db.Base.metadata.create_all(engine)
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    original_session = _db.SessionLocal
    _db.SessionLocal = TestSession

    import routes.agent_hub_routes as agent_routes
    original_agent_session = agent_routes.SessionLocal
    agent_routes.SessionLocal = TestSession

    wakeups = []
    import src.agent_coordinator as coordinator
    monkeypatch.setattr(coordinator, "request_wakeup", lambda: wakeups.append("wake"))

    app = FastAPI()
    from routes.agent_hub_routes import setup_agent_hub_routes
    from routes.a2a_routes import setup_a2a_routes

    app.include_router(setup_agent_hub_routes())
    app.include_router(setup_a2a_routes())

    with TestClient(app) as tc:
        tc.wakeups = wakeups
        yield tc

    _db.SessionLocal = original_session
    agent_routes.SessionLocal = original_agent_session


def _register_agent(client, **overrides):
    body = {
        "id": "agent-1",
        "name": "Odysseus Codex",
        "kind": "cli",
        "adapter_name": "codex",
        "capabilities": ["code-review", "testing"],
        **overrides,
    }
    res = client.post("/api/agent-hub/agents/register", json=body)
    assert res.status_code == 201, res.text
    return res.json()


def test_register_new_agent_returns_token_and_hashes_it(client):
    data = _register_agent(client)
    assert data["id"] == "agent-1"
    assert data["token"]

    db = _db.SessionLocal()
    try:
        agent = db.query(AgentInstance).filter(AgentInstance.id == "agent-1").first()
        assert agent is not None
        assert agent.auth_token_hash != data["token"]
        assert agent.status == "online"
    finally:
        db.close()


def test_reregister_requires_existing_token(client):
    created = _register_agent(client)

    bad = client.post(
        "/api/agent-hub/agents/register",
        headers={"X-Agent-Token": "wrong"},
        json={"id": "agent-1", "name": "Nope", "kind": "cli"},
    )
    assert bad.status_code == 401

    ok = client.post(
        "/api/agent-hub/agents/register",
        headers={"X-Agent-Token": created["token"]},
        json={"id": "agent-1", "name": "Updated", "kind": "cli"},
    )
    assert ok.status_code == 201
    assert ok.json()["name"] == "Updated"
    assert "token" not in ok.json()


def test_list_agents_filters_ownerless_scope(client):
    _register_agent(client)
    res = client.get("/api/agent-hub/agents")
    assert res.status_code == 200
    assert [a["id"] for a in res.json()["agents"]] == ["agent-1"]


def test_agent_card_requires_auth_and_serves_skills(client):
    created = _register_agent(client)
    missing = client.get("/.well-known/agent-card/agent-1")
    assert missing.status_code == 401

    res = client.get(
        "/.well-known/agent-card/agent-1",
        headers={"X-Agent-Token": created["token"]},
    )
    assert res.status_code == 200
    card = res.json()
    assert card["name"] == "Odysseus Codex"
    assert card["protocolVersion"] == "1.0"
    assert {s["id"] for s in card["skills"]} == {"code-review", "testing"}


def test_send_message_creates_agent_task_and_requests_wakeup(client):
    created = _register_agent(client)
    res = client.post(
        "/a2a/send-message",
        headers={"X-Agent-Token": created["token"]},
        json={
            "agent_id": "agent-1",
            "task_id": "remote-task-1",
            "message": {"parts": [{"text": "Review this patch"}]},
        },
    )
    assert res.status_code == 200, res.text
    task = res.json()
    assert task["status"]["state"] == "submitted"
    assert task["metadata"]["external_task_id"] == "remote-task-1"
    assert client.wakeups == ["wake"]

    db = _db.SessionLocal()
    try:
        row = db.query(AgentTask).filter(AgentTask.id == task["id"]).first()
        assert row is not None
        assert row.current_owner == "codex"
        assert row.agent_instance_id == "agent-1"
        assert row.external_protocol == "a2a"
        assert row.objective == "Review this patch"
    finally:
        db.close()


def test_task_status_maps_waiting_for_approval_to_input_required(client):
    created = _register_agent(client)
    send = client.post(
        "/a2a/send-message",
        headers={"X-Agent-Token": created["token"]},
        json={"agent_id": "agent-1", "message": "Needs approval"},
    )
    task_id = send.json()["id"]

    db = _db.SessionLocal()
    try:
        task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
        task.status = "waiting_for_approval"
        db.commit()
    finally:
        db.close()

    res = client.get(
        f"/a2a/tasks/{task_id}/status",
        headers={"X-Agent-Token": created["token"]},
    )
    assert res.status_code == 200
    assert res.json()["status"]["state"] == "input-required"


def test_send_message_maps_a2a_artifacts_to_events(client):
    created = _register_agent(client)
    res = client.post(
        "/a2a/send-message",
        headers={"X-Agent-Token": created["token"]},
        json={
            "agent_id": "agent-1",
            "message": "Return report",
            "artifacts": [
                {
                    "id": "artifact-1",
                    "name": "report.md",
                    "type": "file",
                    "mimeType": "text/markdown",
                    "size": 12,
                    "content": "# Report",
                    "uri": "file:///tmp/report.md",
                }
            ],
        },
    )
    assert res.status_code == 200, res.text
    assert res.json()["artifacts"][0]["name"] == "report.md"

    db = _db.SessionLocal()
    try:
        event = db.query(AgentEvent).filter(AgentEvent.event_type == "artifact").first()
        assert event is not None
        assert event.artifact_type == "file"
        assert event.artifact_mime == "text/markdown"
        assert event.artifact_size == 12
    finally:
        db.close()


def test_create_task_accepts_required_capabilities(client):
    res = client.post(
        "/api/agent-hub/tasks",
        json={
            "title": "Capability task",
            "status": "queued",
            "required_capabilities": ["testing"],
        },
    )
    assert res.status_code == 201, res.text
    assert res.json()["required_capabilities"] == ["testing"]


@pytest.mark.asyncio
async def test_coordinator_matches_task_by_capability(client):
    _register_agent(client, id="agent-cap", adapter_name="mock", capabilities=["testing"])

    class DummyAdapter:
        async def run(self, task, events):
            return AgentAdapterResult(
                summary="done",
                content="matched",
                proposed_status="done",
            )

    import src.agent_coordinator as coordinator

    original_registry = dict(coordinator._adapter_registry)
    original_caps = coordinator.CAPS.copy()
    original_running_total = coordinator._running_total
    original_running_by_adapter = dict(coordinator._running_by_adapter)
    original_running_by_role = dict(coordinator._running_by_role)
    try:
        coordinator._adapter_registry.clear()
        coordinator._adapter_registry["mock"] = DummyAdapter()
        coordinator.CAPS["global"] = 1
        coordinator.CAPS["adapter"] = {"mock": 1}
        coordinator.CAPS["role"] = {}
        coordinator._running_total = 0
        coordinator._running_by_adapter.clear()
        coordinator._running_by_role.clear()

        res = client.post(
            "/api/agent-hub/tasks",
            json={
                "title": "Run tests",
                "status": "queued",
                "required_capabilities": ["testing"],
            },
        )
        assert res.status_code == 201, res.text
        task_id = res.json()["id"]

        await coordinator._tick()

        db = _db.SessionLocal()
        try:
            task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
            assert task.status == "done"
            assert task.current_owner == "mock"
            assert task.agent_instance_id == "agent-cap"
        finally:
            db.close()
    finally:
        coordinator._adapter_registry.clear()
        coordinator._adapter_registry.update(original_registry)
        coordinator.CAPS.clear()
        coordinator.CAPS.update(original_caps)
        coordinator._running_total = original_running_total
        coordinator._running_by_adapter.clear()
        coordinator._running_by_adapter.update(original_running_by_adapter)
        coordinator._running_by_role.clear()
        coordinator._running_by_role.update(original_running_by_role)
