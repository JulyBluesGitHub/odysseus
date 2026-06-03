"""Abstract base classes for agent adapters — result types, probe, actions.

Architecture:
    Adapters **propose** (status, owner, approval, actions).
    Coordinator **decides** (validate transitions, gate dangerous actions).
    User **approves** — only then does the coordinator **execute** actions.

This three-party separation means a broken or malicious adapter can at worst
produce garbage proposals; it cannot execute shell commands, write files, or
make network calls without explicit user approval.
"""

from __future__ import annotations

import logging
import subprocess
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Result types ──────────────────────────────────────────────────────────────


@dataclass
class AgentAction:
    """A single executable action proposed by an adapter.

    Supported types:
        ``shell``       — run a shell command
        ``file_write``  — write content to a file
        ``file_read``   — read a file and return as event content
        ``create_task`` — spawn a subtask (requires role, title fields)
    """

    type: str  # "shell" | "file_write" | "file_read" | "create_task"
    label: str = ""  # human-readable description for the timeline
    # shell
    command: str = ""
    workdir: str | None = None
    # file_write
    path: str = ""
    content: str = ""
    # file_read
    # (uses path above)
    # create_task
    role: str = ""        # diagnoser | implementer | verifier
    task_title: str = ""  # title for the spawned task
    objective: str = ""   # objective for the spawned task
    depends_on: list = field(default_factory=list)  # list of task IDs to wait on


@dataclass
class AgentActionResult:
    """The outcome of executing a single AgentAction."""

    success: bool
    label: str
    output: str = ""
    error: str = ""
    exit_code: int | None = None


@dataclass
class AgentAdapterResult:
    """Structured result from an adapter run.

    Adapters **propose** changes (status, owner, approval flag, actions);
    the coordinator **decides** whether to apply them. This separation means
    a broken adapter can't corrupt task state — it can only produce a result
    that gets logged as an event.
    """

    summary: str
    """One-line summary for the event timeline."""

    content: str
    """Full adapter output / response body."""

    proposed_status: str | None = None
    """Suggested next status (e.g. 'done', 'waiting_for_approval')."""

    proposed_owner: str | None = None
    """Suggested next owner (e.g. 'user', 'hermes')."""

    needs_approval: bool = False
    """If True, the coordinator sets approval_required on the task."""

    metadata: dict[str, Any] = field(default_factory=dict)
    """Arbitrary structured data (token counts, etc.)."""

    actions: list[AgentAction] = field(default_factory=list)
    """Proposed executable actions. Only executed after user approval."""


@dataclass
class AdapterProbe:
    """Result of a capability probe.

    Called once at startup (and cached) so the coordinator knows which adapters
    are available before any task dispatch.
    """

    available: bool
    """True if the adapter can execute right now."""

    cli_path: str | None = None
    """Resolved path to the CLI binary, if applicable."""

    supports_json: bool = False
    """Whether the adapter's tool supports structured JSON output."""

    version: str | None = None
    """Version string of the adapter's backing tool, if discoverable."""

    error: str | None = None
    """Human-readable reason the adapter is unavailable, if applicable."""


class AbstractAdapter(ABC):
    """Every adapter implements ``probe()`` and ``run()``.

    ``probe()`` is called once by the coordinator at startup — the result is
    cached and gates dispatch. ``run()`` receives the task ORM instance and its
    existing event list; it returns a structured ``AgentAdapterResult``.
    """

    @abstractmethod
    async def probe(self) -> AdapterProbe:
        """Check whether this adapter can execute. Called once at startup."""
        ...

    @abstractmethod
    async def run(self, task, events: list) -> AgentAdapterResult:
        """Execute the adapter against a task.

        Args:
            task: ``AgentTask`` ORM instance.
            events: List of ``AgentEvent`` instances (chronological).

        Returns:
            ``AgentAdapterResult`` with summary, content, and proposals.
        """
        ...


# ── Action execution ──────────────────────────────────────────────────────────

# Safety: blocklist of dangerous command patterns that are never allowed,
# even after user approval.
_DANGEROUS_COMMANDS = [
    "rm -rf /", "rm -rf ~", "rm -rf .",
    "dd if=", "mkfs.", ":(){ :|:& };:",  # fork bomb
    "> /dev/sda", "chmod 777 /",
]


def _is_dangerous(command: str) -> bool:
    """Return True if the command matches a known-dangerous pattern."""
    stripped = command.strip().lower()
    for pattern in _DANGEROUS_COMMANDS:
        if pattern in stripped:
            return True
    return False


def execute_action(action: AgentAction, base_dir: str) -> AgentActionResult:
    """Execute a single AgentAction synchronously (called via asyncio.to_thread)."""
    label = action.label or f"{action.type}: {action.command or action.path}"

    try:
        if action.type == "shell":
            return _exec_shell(action, base_dir, label)
        elif action.type == "file_write":
            return _exec_file_write(action, base_dir, label)
        elif action.type == "file_read":
            return _exec_file_read(action, base_dir, label)
        else:
            return AgentActionResult(
                success=False, label=label,
                error=f"Unknown action type: {action.type}",
            )
    except Exception as exc:
        return AgentActionResult(
            success=False, label=label,
            error=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
        )


def _exec_shell(action: AgentAction, base_dir: str, label: str) -> AgentActionResult:
    if _is_dangerous(action.command):
        return AgentActionResult(
            success=False, label=label,
            error="BLOCKED: command matches dangerous pattern",
        )
    workdir = action.workdir or base_dir
    try:
        result = subprocess.run(
            action.command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=workdir,
        )
        output = result.stdout.strip() or result.stderr.strip() or "(no output)"
        return AgentActionResult(
            success=result.returncode == 0,
            label=label,
            output=output[:10000],  # cap to avoid flooding events
            exit_code=result.returncode,
        )
    except subprocess.TimeoutExpired:
        return AgentActionResult(
            success=False, label=label,
            error="Command timed out after 120s",
        )


def _exec_file_write(action: AgentAction, base_dir: str, label: str) -> AgentActionResult:
    path = Path(action.path)
    if not path.is_absolute():
        path = Path(base_dir) / path
    # Safety: refuse to write outside the project directory
    try:
        path.resolve().relative_to(Path(base_dir).resolve())
    except ValueError:
        return AgentActionResult(
            success=False, label=label,
            error=f"BLOCKED: path escapes base directory: {path}",
        )
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(action.content, encoding="utf-8")
        return AgentActionResult(
            success=True, label=label,
            output=f"Wrote {len(action.content)} bytes to {path}",
        )
    except Exception as exc:
        return AgentActionResult(
            success=False, label=label,
            error=f"{type(exc).__name__}: {exc}",
        )


def _exec_file_read(action: AgentAction, base_dir: str, label: str) -> AgentActionResult:
    path = Path(action.path)
    if not path.is_absolute():
        path = Path(base_dir) / path
    try:
        path.resolve().relative_to(Path(base_dir).resolve())
    except ValueError:
        return AgentActionResult(
            success=False, label=label,
            error=f"BLOCKED: path escapes base directory: {path}",
        )
    try:
        content = path.read_text(encoding="utf-8")
        return AgentActionResult(
            success=True, label=label,
            output=content[:10000],
        )
    except Exception as exc:
        return AgentActionResult(
            success=False, label=label,
            error=f"{type(exc).__name__}: {exc}",
        )
