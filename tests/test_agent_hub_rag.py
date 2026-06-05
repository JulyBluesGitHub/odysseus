"""Tests for Agent Hub semantic task search and context injection."""

import math
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

if type(_db.Base).__name__ == "MagicMock":
    pytest.skip("core.database is stubbed", allow_module_level=True)

from fastapi import FastAPI
from fastapi.testclient import TestClient


class FakeCollection:
    def __init__(self):
        self.rows = {}

    def upsert(self, ids, embeddings, metadatas=None, documents=None):
        metadatas = metadatas or [{} for _ in ids]
        documents = documents or ["" for _ in ids]
        for i, row_id in enumerate(ids):
            self.rows[row_id] = {
                "embedding": embeddings[i],
                "metadata": metadatas[i],
                "document": documents[i],
            }

    def delete(self, ids):
        for row_id in ids:
            self.rows.pop(row_id, None)

    def get(self, ids, include=None):
        found_ids = []
        embeddings = []
        metadatas = []
        for row_id in ids:
            if row_id in self.rows:
                found_ids.append(row_id)
                embeddings.append(self.rows[row_id]["embedding"])
                metadatas.append(self.rows[row_id]["metadata"])
        return {"ids": found_ids, "embeddings": embeddings, "metadatas": metadatas}

    def query(self, query_embeddings, n_results=5, include=None):
        query = query_embeddings[0]
        ranked = []
        for row_id, row in self.rows.items():
            distance = _cosine_distance(query, row["embedding"])
            ranked.append((distance, row_id, row))
        ranked.sort(key=lambda item: item[0])
        ranked = ranked[:n_results]
        return {
            "ids": [[row_id for _, row_id, _ in ranked]],
            "metadatas": [[row["metadata"] for _, _, row in ranked]],
            "distances": [[distance for distance, _, _ in ranked]],
        }


def _cosine_distance(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(y * y for y in b))
    if not mag_a or not mag_b:
        return 1.0
    return 1.0 - (dot / (mag_a * mag_b))


def _fake_embedding(text):
    vec = [0.0] * 768
    lowered = text.lower()
    if "login" in lowered:
        vec[0] = 1.0
    if "deploy" in lowered:
        vec[1] = 1.0
    if "test" in lowered:
        vec[2] = 1.0
    if not any(vec):
        vec[3] = 1.0
    return vec


@pytest.fixture()
def fake_rag(monkeypatch):
    import src.agent_hub_rag as rag

    collection = FakeCollection()
    monkeypatch.setattr(rag, "_get_collection", lambda: collection)
    monkeypatch.setattr(rag, "embed_text", _fake_embedding)
    monkeypatch.setattr(rag, "_codebase_context", lambda query, n: [])
    return rag, collection


@pytest.fixture()
def client(monkeypatch, fake_rag):
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
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    original_engine = _db.engine
    original_session = _db.SessionLocal
    _db.engine = engine
    _db.SessionLocal = TestSession

    import routes.agent_hub_routes as agent_hub_routes

    monkeypatch.setattr(agent_hub_routes, "SessionLocal", TestSession)

    app = FastAPI()
    app.include_router(agent_hub_routes.setup_agent_hub_routes())
    with TestClient(app) as tc:
        yield tc

    _db.engine = original_engine
    _db.SessionLocal = original_session


def _create_task(client, **kwargs):
    res = client.post("/api/agent-hub/tasks", json={"title": "Test Task", **kwargs})
    assert res.status_code == 201, res.text
    return res.json()


class TestRAGIndexing:
    def test_index_and_find_similar(self, fake_rag):
        rag, _collection = fake_rag
        rag.index_task("task-a", "Fix login bug", "repair auth flow", "done", "implementer")
        rag.index_task("task-b", "Update login page", "change auth UI", "draft", "diagnoser")

        similar = rag.find_similar("task-a")

        assert any(item["id"] == "task-b" for item in similar)

    def test_delete_removes_from_index(self, fake_rag):
        rag, _collection = fake_rag
        rag.index_task("task-a", "Fix login bug", "repair auth flow", "done", "implementer")
        rag.delete_task_embedding("task-a")

        assert rag.find_similar("task-a") == []

    def test_embedding_dimensions(self, fake_rag):
        rag, _collection = fake_rag

        embedding = rag.embed_text("Fix login")

        assert len(embedding) == 768
        assert all(isinstance(value, float) for value in embedding)


class TestRAGRoutes:
    def test_similar_endpoint(self, client):
        first = _create_task(client, title="Fix login bug", objective="repair auth flow")
        second = _create_task(client, title="Update login page", objective="change auth UI")

        res = client.get(f"/api/agent-hub/tasks/{first['id']}/similar?n=5")

        assert res.status_code == 200
        data = res.json()
        assert data["task_id"] == first["id"]
        assert {"id", "title", "status", "role", "distance"} <= set(data["similar"][0])
        assert any(item["id"] == second["id"] for item in data["similar"])

    def test_similar_nonexistent_task(self, client):
        res = client.get(f"/api/agent-hub/tasks/{uuid.uuid4()}/similar")

        assert res.status_code in (200, 404)
        if res.status_code == 200:
            assert res.json()["similar"] == []


class TestRAGContext:
    def test_rag_context_returns_string(self, fake_rag):
        rag, _collection = fake_rag
        rag.index_task("task-a", "Fix login bug", "repair auth flow", "done", "implementer")

        context = rag.rag_context_for_task("Login failure", "repair auth", n=3)

        assert isinstance(context, str)
        assert "Related tasks" in context
        assert "Fix login bug" in context

    def test_rag_context_empty_on_no_matches(self, fake_rag):
        rag, _collection = fake_rag

        context = rag.rag_context_for_task("Unindexed work", "", n=3)

        assert context == ""
