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

            # Check if the SDK has real error types (not mock fallbacks).
            # When the module is mocked for tests, these will all be missing
            # or set to Exception — skip granular handling in that case.
            _HAS_ERROR_TYPES = all(
                getattr(cursor_sdk, name, None) is not None
                for name in (
                    "APITimeoutError", "InternalServerError", "NetworkError",
                    "PermissionDeniedError", "AuthenticationError",
                )
            )

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
            if not _HAS_ERROR_TYPES:
                # Mock mode or missing error types — broad handling
                logger.exception("Cursor adapter failed")
                return _blocked(f"Cursor error: {exc}", str(exc))

            # Granular error handling with workarounds for cursor-sdk's
            # buggy error hierarchy (see cursor-sdk-bugs-batch-2.md).
            if isinstance(exc, cursor_sdk.APITimeoutError):
                logger.warning("Cursor adapter timeout — retrying")
                return AgentAdapterResult(
                    summary=f"Timeout: {exc}",
                    content=str(exc),
                    proposed_status="queued",
                    needs_approval=False,
                )
            # InternalServerError must come BEFORE NetworkError — cursor-sdk
            # bug: InternalServerError inherits NetworkError (500 is not a
            # network failure, but the hierarchy says it is).
            if isinstance(exc, cursor_sdk.InternalServerError):
                logger.exception("Cursor adapter server error")
                return _blocked(f"Cursor server error: {exc}", str(exc))
            if isinstance(exc, cursor_sdk.NetworkError):
                logger.warning("Cursor adapter network error — retrying")
                return AgentAdapterResult(
                    summary=f"Network error: {exc}",
                    content=str(exc),
                    proposed_status="queued",
                    needs_approval=False,
                )
            # PermissionDeniedError must come BEFORE AuthenticationError —
            # cursor-sdk bug: 403 inherits from 401.
            if isinstance(exc, cursor_sdk.PermissionDeniedError):
                return _blocked(f"Access denied: {exc}", str(exc))
            if isinstance(exc, cursor_sdk.AuthenticationError):
                return _blocked(f"Auth error: {exc}", str(exc))
            if isinstance(exc, cursor_sdk.RateLimitError):
                logger.warning("Cursor adapter rate limited — retrying")
                return AgentAdapterResult(
                    summary=f"Rate limited: {exc}",
                    content=str(exc),
                    proposed_status="queued",
                    needs_approval=False,
                )
            if isinstance(exc, cursor_sdk.ConfigurationError):
                return _blocked(f"Configuration error: {exc}", str(exc))

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
