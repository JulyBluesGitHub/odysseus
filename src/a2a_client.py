"""Thin A2A client adapter for consuming remote A2A agents."""

from __future__ import annotations

import uuid
from typing import Any

import httpx

import core.database as _db
from core.database import AgentInstance
from src.adapters.base import AdapterProbe, AgentAdapterResult, AbstractAdapter
from src.a2a_server import PROTOCOL_VERSION


def skills_to_capabilities(card: dict[str, Any]) -> list[str]:
    capabilities: list[str] = []
    for skill in card.get("skills") or []:
        if isinstance(skill, dict) and isinstance(skill.get("id"), str):
            capabilities.append(skill["id"])
    return capabilities


async def fetch_agent_card(endpoint: str, timeout: float = 10.0) -> dict[str, Any]:
    base = endpoint.rstrip("/")
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(f"{base}/.well-known/agent-card")
        response.raise_for_status()
        card = response.json()
    if not isinstance(card, dict):
        raise ValueError("AgentCard must be a JSON object")
    version = str(card.get("protocolVersion") or card.get("protocol_version") or "")
    if version and version != PROTOCOL_VERSION:
        raise ValueError(f"Unsupported A2A protocol version: {version}")
    return card


async def register_remote_agent(endpoint: str, owner: str | None = None) -> AgentInstance:
    card = await fetch_agent_card(endpoint)
    db = _db.SessionLocal()
    try:
        agent = AgentInstance(
            id=str(uuid.uuid4()),
            owner=owner,
            name=str(card.get("name") or endpoint),
            kind="a2a-remote",
            adapter_name=None,
            status="online",
            capabilities=skills_to_capabilities(card),
            endpoint=endpoint.rstrip("/"),
            agent_card_json=card,
        )
        db.add(agent)
        db.commit()
        db.refresh(agent)
        return agent
    finally:
        db.close()


async def send_message(endpoint: str, agent_id: str, message: str, token: str | None = None) -> dict[str, Any]:
    headers = {"X-Agent-Token": token} if token else {}
    payload = {
        "agent_id": agent_id,
        "message": {"role": "user", "parts": [{"type": "text", "text": message}]},
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{endpoint.rstrip('/')}/a2a/send-message",
            json=payload,
            headers=headers,
        )
        response.raise_for_status()
        return response.json()


async def get_task_status(endpoint: str, task_id: str, token: str | None = None) -> dict[str, Any]:
    headers = {"X-Agent-Token": token} if token else {}
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{endpoint.rstrip('/')}/a2a/tasks/{task_id}/status",
            headers=headers,
        )
        response.raise_for_status()
        return response.json()


def _status_to_agent_hub(state: str | None) -> str:
    return {
        "completed": "done",
        "failed": "blocked",
        "cancelled": "cancelled",
        "input-required": "waiting_for_approval",
        "working": "queued",
        "submitted": "queued",
    }.get(state or "", "queued")


class A2ARemoteAdapter(AbstractAdapter):
    """Coordinator adapter for registered remote A2A agents."""

    async def probe(self) -> AdapterProbe:
        return AdapterProbe(available=True, supports_json=True)

    async def run(self, task, events: list) -> AgentAdapterResult:
        db = _db.SessionLocal()
        try:
            agent = (
                db.query(AgentInstance)
                .filter(AgentInstance.id == task.agent_instance_id)
                .first()
            )
            if not agent or not agent.endpoint:
                return AgentAdapterResult(
                    summary="Remote A2A agent unavailable",
                    content="No endpoint found for matched A2A agent.",
                    proposed_status="blocked",
                )
            response = await send_message(agent.endpoint, agent.id, task.objective or task.title)
            status = response.get("status") or {}
            state = status.get("state") if isinstance(status, dict) else None
            metadata = {
                "a2a_response": response,
                "external_task_id": response.get("id"),
                "artifacts": response.get("artifacts") or [],
            }
            return AgentAdapterResult(
                summary=f"Remote A2A task {state or 'submitted'}",
                content=(status.get("message") if isinstance(status, dict) else None) or "",
                proposed_status=_status_to_agent_hub(state),
                agent_instance_id=agent.id,
                metadata=metadata,
            )
        finally:
            db.close()
