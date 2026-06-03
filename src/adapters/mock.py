"""Mock adapter — echoes the task objective and proposes a next status.

Used to prove the coordinator loop (claim → dispatch → event-write → transition)
before any real agent adapter is wired in. Always returns ``available=True`` and
echoes the task title/objective as the response content.
"""

from __future__ import annotations

from src.adapters.base import AbstractAdapter, AgentAdapterResult, AdapterProbe


class MockAdapter(AbstractAdapter):
    """Echo adapter for testing the coordinator loop.

    Probe always returns available. Run echoes the task's objective back as the
    response content and proposes 'done' as the next status.
    """

    def __init__(self, echo_status: str = "done", echo_label: str = "mock"):
        self._echo_status = echo_status
        self._label = echo_label

    async def probe(self) -> AdapterProbe:
        return AdapterProbe(
            available=True,
            cli_path=None,
            supports_json=False,
            version="1.0.0-mock",
        )

    async def run(self, task, events: list) -> AgentAdapterResult:
        title = getattr(task, "title", "Untitled")
        objective = getattr(task, "objective", "") or "(no objective)"
        return AgentAdapterResult(
            summary=f"[{self._label}] Processed: {title}",
            content=(
                f"Mock adapter received task:\n"
                f"  Title: {title}\n"
                f"  Objective: {objective}\n"
                f"  Events so far: {len(events)}\n"
                f"\n"
                f"This is a mock response. The real adapter would produce "
                f"meaningful output here."
            ),
            proposed_status=self._echo_status,
            proposed_owner="user",
            needs_approval=False,
        )
