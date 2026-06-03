"""Hermes adapter — calls a local Ollama model to process Agent Hub tasks.

Probes Ollama at startup to confirm the API is reachable. Each ``run()`` builds
a structured prompt from the task objective and event history, calls the Ollama
chat API, and parses the response into an ``AgentAdapterResult``.

Env vars:
    OLLAMA_BASE_URL   — defaults to http://127.0.0.1:11434
    HERMES_MODEL      — defaults to qwen2.5-coder:7b
"""

from __future__ import annotations

import json
import logging
import os
import re

import httpx

from src.adapters.base import AbstractAdapter, AgentAdapterResult, AdapterProbe

logger = logging.getLogger(__name__)

DEFAULT_OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
DEFAULT_MODEL = os.getenv("HERMES_MODEL", "qwen2.5-coder:7b")

_SYSTEM_PROMPT = """You are Hermes, an AI assistant working inside the Odysseus Agent Hub.
You receive tasks from a user and produce structured responses.

For each task you receive:
1. The task TITLE is the primary instruction. If the objective field is empty, use the title as your directive.
2. Read the event history for context from previous steps.
3. Produce a thoughtful response addressing the task.
4. End your response with [STATUS: xxx] followed by an optional [ACTIONS] block.

Valid statuses:
- done — task completed, no further actions needed
- waiting_for_approval — actions proposed, awaiting user approval
- blocked — task CANNOT be completed (missing critical info, impossible request). Do NOT block just because the objective field is empty — use the title.
- queued — needs another agent or retry

IMPORTANT: create_task actions. When asked to spawn subtasks for other agent roles (diagnoser, implementer, verifier), use create_task actions in the [ACTIONS] block. Each create_task action becomes a real queued task that other agents will pick up. Do NOT use file_write or shell actions to describe subtask work — use create_task for that.

[ACTIONS]
{"type": "create_task", "label": "Spawn implementation", "role": "implementer", "title": "Implement X", "objective": "Detailed instructions for the implementer", "depends_on": "parent-task-id"}
{"type": "file_write", "label": "Create main.py", "path": "src/main.py", "content": "print('hello')"}
{"type": "shell", "label": "Run the script", "command": "python src/main.py"}

create_task fields:
- role (required): diagnoser, implementer, or verifier
- title (required): short task title
- objective (optional): detailed instructions for the child task
- depends_on (optional): task ID this subtask must wait for (string, not array — use the current task's ID)

file_write / shell actions are for YOUR work only. Use create_task to delegate to other roles.

Rules for actions:
- file_write: path is relative to the project root. Include the full file content.
- shell: command is a single shell command. Use full paths when possible.
- NEVER propose rm -rf, dd, mkfs, chmod 777, or fork bombs — they are blocked.
- Each action gets an approval event in the timeline.

Be concise but thorough. Include code snippets in your response when relevant.
If you only provide analysis (no file changes or commands), use [STATUS: done].
"""


class HermesAdapter(AbstractAdapter):
    """Calls a local Ollama model to process Agent Hub tasks."""

    def __init__(self, ollama_url: str | None = None, model: str | None = None):
        self._ollama_url = (ollama_url or DEFAULT_OLLAMA_URL).rstrip("/")
        self._model = model or DEFAULT_MODEL
        self._chat_url = f"{self._ollama_url}/api/chat"

    async def probe(self) -> AdapterProbe:
        """Check whether Ollama is reachable and the model is available."""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(f"{self._ollama_url}/api/tags")
                if not r.is_success:
                    return AdapterProbe(
                        available=False,
                        error=f"Ollama returned {r.status_code}",
                    )
                data = r.json()
                models = [m.get("name", "") for m in data.get("models", [])]
                if self._model not in models:
                    return AdapterProbe(
                        available=False,
                        error=f"Model '{self._model}' not found. Available: {', '.join(models[:8])}",
                    )
                return AdapterProbe(
                    available=True,
                    cli_path=self._ollama_url,
                    supports_json=False,
                    version="ollama",
                )
        except httpx.ConnectError:
            return AdapterProbe(
                available=False,
                error=f"Cannot connect to Ollama at {self._ollama_url}",
            )
        except Exception as exc:
            return AdapterProbe(
                available=False,
                error=f"Probe failed: {exc}",
            )

    async def run(self, task, events: list) -> AgentAdapterResult:
        """Build a prompt from the task and events, call Ollama, parse the result."""
        title = getattr(task, "title", "Untitled")
        objective = getattr(task, "objective", "") or "(no objective provided)"

        # Build the conversation
        messages = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_message(title, objective, events)},
        ]

        try:
            response_text = await _call_ollama(self._chat_url, self._model, messages)
        except Exception as exc:
            logger.exception("Hermes adapter: Ollama call failed")
            return AgentAdapterResult(
                summary=f"Ollama error: {exc}",
                content=str(exc),
                proposed_status="queued",  # retry
                needs_approval=False,
            )

        # Parse the status from the response
        proposed_status = _extract_status(response_text) or "done"
        summary = _extract_summary(response_text, title)

        # Parse any proposed actions
        actions = _extract_actions(response_text)

        return AgentAdapterResult(
            summary=summary,
            content=response_text,
            proposed_status=proposed_status,
            proposed_owner="user",
            needs_approval=(proposed_status == "waiting_for_approval"),
            actions=actions,
        )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_user_message(title: str, objective: str, events: list) -> str:
    """Construct the user message from task data and event history."""
    parts = [
        f"## Task: {title}",
        f"",
        f"### Objective",
        objective,
    ]
    if events:
        parts.append("")
        parts.append("### Event History")
        for e in events[-20:]:  # last 20 events
            actor = getattr(e, "actor", "unknown")
            summary = getattr(e, "summary", "") or ""
            parts.append(f"- [{actor}] {summary}")
    parts.append("")
    parts.append("Respond to this task. Include a [STATUS: ...] line at the end.")
    return "\n".join(parts)


_STATUS_RE = re.compile(r"\[STATUS:\s*(\w+)\s*\]", re.IGNORECASE)
_ACTIONS_RE = re.compile(r"\[ACTIONS\]\s*\n(.*?)(?=\[|\Z)", re.DOTALL | re.IGNORECASE)

_VALID_PROPOSED = {"done", "waiting_for_approval", "blocked", "queued"}


def _extract_status(text: str) -> str | None:
    """Extract [STATUS: xxx] from the end of a response."""
    matches = _STATUS_RE.findall(text)
    if matches:
        status = matches[-1].strip().lower()
        if status in _VALID_PROPOSED:
            return status
    return None


def _extract_actions(text: str) -> list:
    """Extract JSON action objects from an [ACTIONS] block in the response."""
    from src.adapters.base import AgentAction

    match = _ACTIONS_RE.search(text)
    if not match:
        return []

    block = match.group(1).strip()
    actions = []
    for line in block.split("\n"):
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
            action = AgentAction(
                type=obj.get("type", ""),
                label=obj.get("label", ""),
                command=obj.get("command", ""),
                path=obj.get("path", ""),
                content=obj.get("content", ""),
                workdir=obj.get("workdir"),
                role=obj.get("role", ""),
                task_title=obj.get("title") or obj.get("description", ""),
                objective=obj.get("objective") or obj.get("details", ""),
                depends_on=obj.get("depends_on") if isinstance(obj.get("depends_on"), list) else ([obj["depends_on"]] if isinstance(obj.get("depends_on"), str) and obj.get("depends_on") else []),
            )
            # Only include valid action types
            if action.type in ("shell", "file_write", "file_read", "create_task"):
                actions.append(action)
        except (json.JSONDecodeError, TypeError, KeyError):
            continue
    return actions


def _extract_summary(text: str, title: str) -> str:
    """Extract a one-line summary from the response."""
    # Take the first non-empty line that isn't a heading
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "[STATUS:" not in stripped:
            if len(stripped) > 120:
                return stripped[:117] + "..."
            return stripped
    return f"Response to: {title}"


async def _call_ollama(chat_url: str, model: str, messages: list) -> str:
    """Call the Ollama chat API and return the response text."""
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": 0.3,
            "num_predict": 2048,
        },
    }
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(chat_url, json=payload)
        if not r.is_success:
            raise RuntimeError(f"Ollama returned {r.status_code}: {r.text[:300]}")
        data = r.json()
        return (data.get("message") or {}).get("content", "") or str(data)
