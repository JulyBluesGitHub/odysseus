"""Agent adapter package — base classes and concrete adapters.

Exports:
    AgentAdapterResult  — structured adapter output
    AgentAction         — a single executable action (shell, file_write, file_read)
    AdapterProbe        — capability check result
    AbstractAdapter     — base class with probe() + run()
    MockAdapter         — echo adapter for testing the coordinator loop
    HermesAdapter       — calls local Ollama model
    CodexAdapter        — calls Codex CLI (guarded by capability probe)
"""

from src.adapters.base import (
    AgentAdapterResult,
    AgentAction,
    AgentActionResult,
    AdapterProbe,
    AbstractAdapter,
)
from src.adapters.mock import MockAdapter
from src.adapters.hermes import HermesAdapter
from src.adapters.codex import CodexAdapter

__all__ = [
    "AgentAdapterResult", "AgentAction", "AgentActionResult",
    "AdapterProbe", "AbstractAdapter",
    "MockAdapter", "HermesAdapter", "CodexAdapter",
]
