"""Cursor adapter — cursor-sdk integration for Agent Hub tasks."""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
from pathlib import Path

# Windows: cursor-sdk uses Unix-only os functions. Patch before any import.
if not hasattr(os, "get_blocking"):
    os.get_blocking = lambda fd: True
if not hasattr(os, "set_blocking"):
    os.set_blocking = lambda fd, blocking: None

from src.adapters.base import AbstractAdapter, AgentAdapterResult, AdapterProbe

logger = logging.getLogger(__name__)

CURSOR_SANDBOX_MODES = {"read-only", "workspace-write", "danger-full-access"}


class CursorAdapter(AbstractAdapter):
    """Runs Agent Hub tasks through cursor-sdk local agent mode."""

    def __init__(self, model: str = "composer-2.5"):
        self._model = model

    async def probe(self) -> AdapterProbe:
        """Check whether cursor-sdk is importable and auth is configured."""
        try:
            cursor_sdk = importlib.import_module("cursor_sdk")
        except ImportError:
            return AdapterProbe(
                available=False,
                error="cursor-sdk not installed",
            )
        except Exception as exc:
            return AdapterProbe(
                available=False,
                error=f"Probe failed: {exc}",
            )

        if not _has_cursor_auth():
            return AdapterProbe(
                available=False,
                error="CURSOR_API_KEY not set",
            )

        return AdapterProbe(
            available=True,
            supports_json=False,
            version=getattr(cursor_sdk, "version", None),
        )

    async def run(
        self,
        task,
        events: list | None = None,
        workspace: str | None = None,
        sandbox_mode: str | None = None,
    ) -> AgentAdapterResult:
        """Run the task through Cursor's local agent."""
        sandbox_name = _sandbox_name(task, sandbox_mode)
        if sandbox_name not in CURSOR_SANDBOX_MODES:
            return _blocked(
                f"Invalid sandbox_mode: {sandbox_name}",
                f"Task sandbox_mode '{sandbox_name}' is not one of "
                f"{sorted(CURSOR_SANDBOX_MODES)}.",
            )

        prompt = (
            getattr(task, "title", None)
            or getattr(task, "objective", None)
            or ""
        )
        cwd = workspace or os.getcwd()

        try:
            cursor_sdk = importlib.import_module("cursor_sdk")
            Agent = cursor_sdk.Agent
            AgentOptions = cursor_sdk.AgentOptions
            LocalAgentOptions = cursor_sdk.LocalAgentOptions

            result = await asyncio.to_thread(
                Agent.prompt,
                prompt,
                options=AgentOptions(
                    model=self._model,
                    local=LocalAgentOptions(cwd=cwd),
                ),
            )
            return AgentAdapterResult(
                summary=str(result)[:200],
                content=str(result),
                proposed_status="done",
                proposed_owner="user",
                needs_approval=False,
            )
        except Exception as exc:
            logger.exception("Cursor adapter failed")
            return _blocked(f"Cursor error: {exc}", str(exc))


def _has_cursor_auth() -> bool:
    return bool(os.environ.get("CURSOR_API_KEY")) or (
        Path.home() / ".cursor" / "auth.json"
    ).exists()


def _sandbox_name(task, explicit: str | None) -> str:
    value = (
        explicit
        if explicit is not None
        else getattr(task, "sandbox_mode", None)
    )
    return value if isinstance(value, str) and value else "workspace-write"


def _blocked(summary: str, content: str) -> AgentAdapterResult:
    return AgentAdapterResult(
        summary=summary,
        content=content,
        proposed_status="blocked",
        needs_approval=False,
    )
