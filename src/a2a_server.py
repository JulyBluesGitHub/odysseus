"""A2A compatibility helpers for Agent Hub.

This module keeps A2A at the protocol boundary: it translates Agent Cards,
SendMessage payloads, and task status objects to and from existing Agent Hub
models without changing the coordinator's internal state machine.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

import bcrypt
from fastapi import HTTPException

import core.database as _db
from core.database import AgentEvent, AgentInstance, AgentTask


PROTOCOL_VERSION = "1.0"

_STATUS_TO_A2A = {
    "draft": "submitted",
    "scheduled": "submitted",
    "paused": "submitted",
    "queued": "submitted",
    "running": "working",
    "waiting_for_approval": "input-required",
    "blocked": "failed",
    "done": "completed",
    "cancelled": "cancelled",
}


def verify_agent_token(agent: AgentInstance, token: str | None) -> bool:
    if not token or not agent.auth_token_hash:
        return False
    try:
        return bcrypt.checkpw(token.encode("utf-8"), agent.auth_token_hash.encode("utf-8"))
    except Exception:
        return False


def require_agent_auth(agent: AgentInstance, token: str | None) -> None:
    if not verify_agent_token(agent, token):
        raise HTTPException(401, "Invalid agent token")


def task_status_to_a2a(status: str | None) -> str:
    return _STATUS_TO_A2A.get(status or "", "working")


def _skill_from_capability(capability: str) -> dict[str, str]:
    name = capability.replace("-", " ").replace("_", " ").title()
    return {
        "id": capability,
        "name": name,
        "description": f"{name} capability",
    }


def build_agent_card(agent: AgentInstance, base_url: str = "") -> dict[str, Any]:
    """Build an A2A AgentCard for a registered agent."""
    if agent.agent_card_json:
        card = dict(agent.agent_card_json)
        card.setdefault("protocolVersion", PROTOCOL_VERSION)
        card.setdefault("url", agent.endpoint or f"{base_url}/a2a/agent/{agent.id}")
        return card

    skills = [_skill_from_capability(c) for c in (agent.capabilities or [])]
    url = agent.endpoint or f"{base_url}/a2a/agent/{agent.id}"
    description = (
        f"{agent.adapter_name} adapter exposed through Odysseus Agent Hub"
        if agent.adapter_name else
        "A2A agent registered with Odysseus Agent Hub"
    )
    return {
        "name": agent.name,
        "description": description,
        "url": url,
        "provider": {"organization": "Odysseus Agent Hub"},
        "capabilities": {"streaming": True, "pushNotifications": True},
        "skills": skills,
        "defaultInputModes": ["text", "file"],
        "defaultOutputModes": ["text", "file"],
        "securitySchemes": {
            "apiKey": {"type": "apiKey", "in": "header", "name": "X-Agent-Token"}
        },
        "protocolVersion": PROTOCOL_VERSION,
    }


def _extract_text_message(payload: dict[str, Any]) -> str:
    message = payload.get("message")
    if isinstance(message, str):
        return message
    if isinstance(message, dict):
        parts = message.get("parts")
        if isinstance(parts, list):
            texts = []
            for part in parts:
                if not isinstance(part, dict):
                    continue
                text = part.get("text") or part.get("content")
                if isinstance(text, str):
                    texts.append(text)
            if texts:
                return "\n".join(texts)
        content = message.get("content")
        if isinstance(content, str):
            return content
    params = payload.get("params")
    if isinstance(params, dict):
        return _extract_text_message(params)
    return ""


def _payload_field(payload: dict[str, Any], name: str, default: Any = None) -> Any:
    if name in payload:
        return payload.get(name)
    params = payload.get("params")
    if isinstance(params, dict):
        return params.get(name, default)
    return default


def _extract_artifacts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = _payload_field(payload, "artifacts", [])
    if not isinstance(artifacts, list):
        return []
    return [a for a in artifacts if isinstance(a, dict)]


def _artifact_to_event(task_id: str, actor: str, artifact: dict[str, Any]) -> AgentEvent:
    content = artifact.get("content")
    if not isinstance(content, str):
        content = json.dumps(content, default=str) if content is not None else None
    metadata = {
        "artifact_id": artifact.get("id"),
        "uri": artifact.get("uri"),
        "parts": artifact.get("parts"),
        "metadata": artifact.get("metadata"),
    }
    return AgentEvent(
        id=str(uuid.uuid4()),
        task_id=task_id,
        actor=actor,
        event_type="artifact",
        summary=str(artifact.get("name") or artifact.get("summary") or "A2A artifact"),
        content=content,
        metadata_json=json.dumps(metadata, default=str),
        artifact_type=artifact.get("type"),
        artifact_mime=artifact.get("mime_type") or artifact.get("mimeType") or artifact.get("mime"),
        artifact_size=artifact.get("size"),
    )


def create_task_from_send_message(payload: dict[str, Any], token: str | None) -> dict[str, Any]:
    """Create an AgentTask from an A2A SendMessage-style payload."""
    agent_id = _payload_field(payload, "agent_id") or _payload_field(payload, "agentId")
    if not isinstance(agent_id, str) or not agent_id.strip():
        raise HTTPException(400, "agent_id is required")
    external_task_id = _payload_field(payload, "task_id") or _payload_field(payload, "taskId")
    context_id = _payload_field(payload, "context_id") or _payload_field(payload, "contextId")
    response_to_task_id = (
        _payload_field(payload, "response_to_task_id")
        or _payload_field(payload, "responseToTaskId")
    )
    required_capabilities = (
        _payload_field(payload, "required_capabilities")
        or _payload_field(payload, "requiredCapabilities")
        or []
    )
    if not isinstance(required_capabilities, list):
        raise HTTPException(400, "required_capabilities must be a list")
    text = _extract_text_message(payload).strip()
    if not text:
        raise HTTPException(400, "message text is required")

    db = _db.SessionLocal()
    try:
        agent = db.query(AgentInstance).filter(AgentInstance.id == agent_id).first()
        if not agent:
            raise HTTPException(404, "Agent not found")
        require_agent_auth(agent, token)
        if agent.status == "offline":
            raise HTTPException(409, "Agent is offline")

        task = AgentTask(
            id=str(uuid.uuid4()),
            owner=agent.owner,
            title=(text.splitlines()[0] or "A2A message")[:120],
            objective=text,
            status="queued",
            current_owner=agent.adapter_name,
            agent_instance_id=agent.id,
            external_protocol="a2a",
            external_task_id=external_task_id,
            agent_card_url=agent.endpoint,
            response_to_task_id=response_to_task_id,
            required_capabilities=required_capabilities,
            tags=["a2a"],
        )
        db.add(task)
        db.flush()
        event = AgentEvent(
            id=str(uuid.uuid4()),
            task_id=task.id,
            actor=agent.id,
            event_type="message",
            summary="A2A SendMessage received",
            content=text,
            metadata_json=json.dumps({
                "external_task_id": external_task_id,
                "context_id": context_id,
                "agent_instance_id": agent.id,
            }, default=str),
        )
        db.add(event)
        for artifact in _extract_artifacts(payload):
            db.add(_artifact_to_event(task.id, agent.id, artifact))
        db.commit()
        db.refresh(task)

        try:
            from src.agent_hub_events import publish
            from src.agent_hub_events import _task_to_ssedict
            publish(agent.owner or "", "task_created", _task_to_ssedict(task))
        except Exception:
            pass
        try:
            from src.agent_coordinator import request_wakeup
            request_wakeup()
        except Exception:
            pass
        return task_to_a2a_task(task)
    finally:
        db.close()


def task_to_a2a_task(task: AgentTask) -> dict[str, Any]:
    status = task_status_to_a2a(task.status)
    artifacts = [
        {
            "id": event.id,
            "name": event.summary,
            "type": event.artifact_type,
            "mimeType": event.artifact_mime,
            "size": event.artifact_size,
            "content": event.content,
            "metadata": _safe_json_load(event.metadata_json),
        }
        for event in (task.events or [])
        if event.event_type == "artifact" or event.artifact_type
    ]
    return {
        "id": task.id,
        "contextId": task.session_id or task.response_to_task_id,
        "status": {
            "state": status,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "message": task.last_error if status == "failed" else None,
        },
        "metadata": {
            "owner": task.owner,
            "agent_instance_id": task.agent_instance_id,
            "external_protocol": task.external_protocol,
            "external_task_id": task.external_task_id,
            "required_capabilities": task.required_capabilities or [],
        },
        "artifacts": artifacts,
    }


def _safe_json_load(raw: str | None) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def get_agent_card(agent_id: str, token: str | None, base_url: str = "") -> dict[str, Any]:
    db = _db.SessionLocal()
    try:
        agent = db.query(AgentInstance).filter(AgentInstance.id == agent_id).first()
        if not agent or agent.status == "offline":
            raise HTTPException(404, "Agent not found")
        require_agent_auth(agent, token)
        return build_agent_card(agent, base_url=base_url)
    finally:
        db.close()


def get_task_status(task_id: str, token: str | None) -> dict[str, Any]:
    db = _db.SessionLocal()
    try:
        task = db.query(AgentTask).filter(AgentTask.id == task_id).first()
        if not task:
            raise HTTPException(404, "Task not found")
        if task.agent_instance_id:
            agent = (
                db.query(AgentInstance)
                .filter(AgentInstance.id == task.agent_instance_id)
                .first()
            )
            if agent:
                require_agent_auth(agent, token)
        return task_to_a2a_task(task)
    finally:
        db.close()
