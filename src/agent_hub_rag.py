"""Semantic task search and context assembly for Agent Hub."""

from __future__ import annotations

import json
import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

COLLECTION_NAME = "odysseus_agent_hub_tasks"
EMBEDDING_MODEL = "nomic-embed-text"
OLLAMA_EMBED_URL = "http://localhost:11434/v1/embeddings"


def _get_collection():
    """Get or create the Agent Hub task collection."""
    from src.chroma_client import get_chroma_client

    client = get_chroma_client()
    if hasattr(client, "get_or_create_collection"):
        return client.get_or_create_collection(
            COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )

    # Compatibility with ChromaDB client variants that only expose names from
    # list_collections in v0.6.x.
    try:
        return client.get_collection(COLLECTION_NAME)
    except Exception:
        return client.create_collection(
            COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )


def embed_text(text: str) -> list[float]:
    """Get an embedding vector for text via Ollama."""
    r = requests.post(
        OLLAMA_EMBED_URL,
        json={"model": EMBEDDING_MODEL, "input": [text]},
        timeout=(3, 20),
    )
    r.raise_for_status()
    data = r.json()
    if "embeddings" in data:
        return data["embeddings"][0]
    embeddings = data.get("data") or []
    embeddings.sort(key=lambda item: item.get("index", 0))
    if embeddings:
        return embeddings[0]["embedding"]
    raise ValueError("embedding response did not include embeddings")


def _task_text(title: str, objective: str | None, status: str, role: str | None) -> str:
    return f"Role: {role or ''}. Title: {title}. Objective: {objective or ''}. Status: {status}"


def index_task(task_id: str, title: str, objective: str, status: str, role: str):
    """Index a task for semantic search. Best-effort for CRUD callers."""
    try:
        text = _task_text(title, objective, status, role)
        embedding = embed_text(text)
        collection = _get_collection()
        collection.upsert(
            ids=[task_id],
            embeddings=[embedding],
            documents=[text],
            metadatas=[{"title": title, "status": status, "role": role or ""}],
        )
    except Exception as exc:
        logger.warning("Agent Hub RAG index failed for task %s: %s", task_id, exc)


def delete_task_embedding(task_id: str):
    """Remove a task from the vector index."""
    try:
        _get_collection().delete(ids=[task_id])
    except Exception as exc:
        logger.debug("Agent Hub RAG delete skipped for task %s: %s", task_id, exc)


def find_similar(task_id: str, n: int = 5) -> list[dict]:
    """Find the N most similar indexed tasks by embedding similarity."""
    try:
        n = max(1, min(int(n), 20))
    except (TypeError, ValueError):
        n = 5

    try:
        collection = _get_collection()
        results = collection.get(ids=[task_id], include=["embeddings"])
        embeddings = results.get("embeddings") if results else None
        if embeddings is None or len(embeddings) == 0:
            return []
        query_embedding = embeddings[0]
        similar = collection.query(
            query_embeddings=[query_embedding],
            n_results=n + 1,
            include=["metadatas", "distances"],
        )
        ids = (similar.get("ids") or [[]])[0]
        metadatas = (similar.get("metadatas") or [[]])[0]
        distances = (similar.get("distances") or [[]])[0]

        out = []
        for i, sid in enumerate(ids):
            if sid == task_id:
                continue
            meta = metadatas[i] if i < len(metadatas) and metadatas[i] else {}
            out.append({
                "id": sid,
                "title": meta.get("title", ""),
                "status": meta.get("status", ""),
                "role": meta.get("role", ""),
                "distance": distances[i] if i < len(distances) else None,
            })
        return out[:n]
    except Exception as exc:
        logger.debug("Agent Hub similar search failed for task %s: %s", task_id, exc)
        return []


def _codebase_context(query: str, n: int) -> list[str]:
    try:
        from src.rag_singleton import get_rag_manager

        rag = get_rag_manager()
        if not rag:
            return []
        chunks = rag.search(query, k=n)
    except Exception as exc:
        logger.debug("Agent Hub codebase RAG context unavailable: %s", exc)
        return []

    lines = []
    for item in chunks[:n]:
        if not isinstance(item, dict):
            continue
        meta = item.get("metadata") or {}
        source = meta.get("source") or meta.get("path") or item.get("source") or "codebase"
        text = item.get("text") or item.get("content") or item.get("document") or ""
        if text:
            lines.append(f"  - {source}: {str(text).strip()[:500]}")
    return lines


def rag_context_for_task(task_title: str, task_objective: str, n: int = 3) -> str:
    """Build a RAG context string for injection into agent briefs."""
    query = f"Title: {task_title}. Objective: {task_objective or ''}"
    lines: list[str] = []

    try:
        embedding = embed_text(query)
        collection = _get_collection()
        results = collection.query(
            query_embeddings=[embedding],
            n_results=max(1, min(int(n), 10)),
            include=["metadatas", "distances"],
        )
        ids = (results.get("ids") or [[]])[0]
        metadatas = (results.get("metadatas") or [[]])[0]
        if ids:
            lines.append("Related tasks (from vector memory):")
            for i, tid in enumerate(ids):
                meta = metadatas[i] if i < len(metadatas) and metadatas[i] else {}
                lines.append(f"  - {meta.get('title', tid)} [{meta.get('status', '?')}]")
    except Exception as exc:
        logger.debug("Agent Hub task RAG context unavailable: %s", exc)

    codebase_lines = _codebase_context(query, n)
    if codebase_lines:
        if lines:
            lines.append("")
        lines.append("Relevant codebase context:")
        lines.extend(codebase_lines)

    return "\n".join(lines)


def as_json(data: Any) -> str:
    """Serialize test/debug data without leaking non-serializable objects."""
    return json.dumps(data, default=str)
