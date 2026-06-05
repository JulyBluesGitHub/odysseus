"""A2A protocol boundary routes for Agent Hub."""

from __future__ import annotations

import asyncio
import json
from typing import Optional

from fastapi import APIRouter, Header, Request
from fastapi.responses import StreamingResponse

from src.a2a_server import create_task_from_send_message, get_agent_card, get_task_status
from src.agent_hub_events import subscribe as _hub_subscribe


def setup_a2a_routes() -> APIRouter:
    router = APIRouter(tags=["a2a"])

    @router.get("/.well-known/agent-card/{agent_id}")
    async def agent_card(
        request: Request,
        agent_id: str,
        x_agent_token: Optional[str] = Header(None),
    ):
        base_url = str(request.base_url).rstrip("/")
        return get_agent_card(agent_id, x_agent_token, base_url=base_url)

    @router.post("/a2a/send-message")
    async def send_message(request: Request, x_agent_token: Optional[str] = Header(None)):
        payload = await request.json()
        return create_task_from_send_message(payload, x_agent_token)

    @router.get("/a2a/tasks/{task_id}/status")
    async def task_status(task_id: str, x_agent_token: Optional[str] = Header(None)):
        return get_task_status(task_id, x_agent_token)

    @router.get("/a2a/tasks/stream/{owner}")
    async def task_stream(request: Request, owner: str):
        """A2A-compatible SSE bridge over the existing Agent Hub owner stream."""

        async def _events():
            async for raw in _hub_subscribe(owner, request):
                if raw.startswith(":"):
                    yield raw
                    continue
                event_name = None
                data_payload = None
                for line in raw.splitlines():
                    if line.startswith("event:"):
                        event_name = line.split(":", 1)[1].strip()
                    elif line.startswith("data:"):
                        data_payload = line.split(":", 1)[1].strip()
                if not data_payload:
                    continue
                try:
                    data = json.loads(data_payload)
                except json.JSONDecodeError:
                    continue
                yield (
                    "event: TaskStatusUpdateEvent\n"
                    f"data: {json.dumps({'type': event_name, 'data': data}, default=str)}\n\n"
                )
                await asyncio.sleep(0)

        return StreamingResponse(
            _events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return router
