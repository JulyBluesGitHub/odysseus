"""Codex adapter — OpenAI Codex Python SDK integration."""

from __future__ import annotations

import importlib
import logging
from typing import Any

from src.adapters.base import AbstractAdapter, AgentAdapterResult, AdapterProbe

logger = logging.getLogger(__name__)

CODEX_SANDBOX_MODES = {"read-only", "workspace-write", "danger-full-access"}


class CodexAdapter(AbstractAdapter):
    """Runs Agent Hub tasks through the openai-codex SDK."""

    def __init__(self, cli_command: str | None = None):
        self._cli = cli_command
        self._probe_cache: AdapterProbe | None = None

    async def probe(self) -> AdapterProbe:
        """Check whether the openai-codex SDK can be imported."""
        if self._probe_cache is not None:
            return self._probe_cache

        if self._cli and self._cli != "codex":
            self._probe_cache = AdapterProbe(
                available=False,
                error=f"'{self._cli}' not found on PATH",
            )
            return self._probe_cache

        try:
            openai_codex = importlib.import_module("openai_codex")
        except ImportError:
            self._probe_cache = AdapterProbe(
                available=False,
                error="openai-codex not installed",
            )
            return self._probe_cache
        except Exception as exc:
            self._probe_cache = AdapterProbe(
                available=False,
                error=f"Probe failed: {exc}",
            )
            return self._probe_cache

        self._probe_cache = AdapterProbe(
            available=True,
            supports_json=False,
            version=getattr(openai_codex, "version", None),
        )
        return self._probe_cache

    async def run(
        self,
        task,
        events: list | None = None,
        workspace: str | None = None,
        sandbox_mode: str | None = None,
    ) -> AgentAdapterResult:
        """Run the task through an AsyncCodex thread."""
        sandbox_name = _sandbox_name(task, sandbox_mode)
        if sandbox_name not in CODEX_SANDBOX_MODES:
            return _blocked(
                f"Invalid sandbox_mode: {sandbox_name}",
                (
                    f"Task sandbox_mode '{sandbox_name}' is not one of "
                    f"{sorted(CODEX_SANDBOX_MODES)}."
                ),
            )

        probe = await self.probe()
        if not probe.available:
            return _blocked(
                f"Codex unavailable: {probe.error}",
                (
                    "Codex SDK could not be imported.\n"
                    f"  Error: {probe.error}\n\n"
                    "Install it in the Odysseus venv with: pip install openai-codex"
                ),
            )

        prompt = _task_prompt(task)

        try:
            openai_codex = importlib.import_module("openai_codex")
            AsyncCodex = openai_codex.AsyncCodex
            sandbox = _sandbox_from_name(openai_codex.Sandbox, sandbox_name)

            async with AsyncCodex() as codex:
                thread = await codex.thread_start(sandbox=sandbox, cwd=workspace)
                result = await thread.run(prompt)

            output = getattr(result, "final_response", "") or ""
            return AgentAdapterResult(
                summary=_summary(output, getattr(task, "title", "Untitled")),
                content=output,
                proposed_status="done",
                proposed_owner="user",
                needs_approval=False,
                metadata={
                    "usage": getattr(result, "usage", None),
                    "items": _serialisable_items(getattr(result, "items", None)),
                },
            )
        except Exception as exc:
            logger.exception("Codex adapter failed")
            return _blocked(f"Codex error: {exc}", str(exc))


def _sandbox_from_name(sandbox_cls: Any, sandbox_name: str):
    return {
        "read-only": sandbox_cls.read_only,
        "workspace-write": sandbox_cls.workspace_write,
        "danger-full-access": sandbox_cls.full_access,
    }[sandbox_name]


def _sandbox_name(task, explicit: str | None) -> str:
    value = explicit if explicit is not None else getattr(task, "sandbox_mode", None)
    return value if isinstance(value, str) and value else "workspace-write"


def _task_prompt(task) -> str:
    return (
        getattr(task, "title", None)
        or getattr(task, "objective", None)
        or ""
    )


def _summary(output: str, title: str) -> str:
    for line in output.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:200]
    return f"Codex response to: {title}"


def _serialisable_items(items):
    if items is None:
        return None
    try:
        return list(items)
    except TypeError:
        return str(items)


def _blocked(summary: str, content: str) -> AgentAdapterResult:
    return AgentAdapterResult(
        summary=summary,
        content=content,
        proposed_status="blocked",
        needs_approval=False,
    )
