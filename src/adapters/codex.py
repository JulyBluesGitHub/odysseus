"""Codex adapter — OpenAI Codex CLI integration.

Installed via ``npm install -g @openai/codex``. On Windows the npm global
binary is a .cmd wrapper; this adapter resolves the full path with ``where``
and uses it for all subprocess invocations to avoid shell-resolution issues.

The adapter:
1. Probes: resolves the CLI path, checks it's not a sandboxed WindowsApps binary,
   verifies ``codex exec --help`` succeeds, and detects ``--json`` support.
2. Executes: calls ``<resolved_path> exec --json "<prompt>"`` (plain-text fallback
   if --json unavailable).
3. NEVER executes returned commands directly — output is stored as an event only.
"""

from __future__ import annotations

import logging
import shutil
import subprocess

from src.adapters.base import AbstractAdapter, AgentAdapterResult, AdapterProbe

logger = logging.getLogger(__name__)

# Known WindowsApps paths that fail with Access is denied
_BLOCKED_PREFIXES = (
    "C:\\Program Files\\WindowsApps\\",
)

# Valid sandbox modes accepted by Codex CLI --sandbox flag
CODEX_SANDBOX_MODES = {"read-only", "workspace-write", "danger-full-access"}


class CodexAdapter(AbstractAdapter):
    """Guarded Codex CLI adapter. Probe must pass before any dispatch."""

    def __init__(self, cli_command: str = "codex"):
        self._cli = cli_command
        self._resolved_path: str | None = None
        self._probe_cache: AdapterProbe | None = None

    async def probe(self) -> AdapterProbe:
        """Check whether the Codex CLI is reachable and supports --json.

        Caches the result so repeated calls don't keep spawning subprocesses.
        """
        if self._probe_cache is not None:
            return self._probe_cache

        try:
            # Resolve the binary path (shutil.which handles Windows PATHEXT correctly)
            cli_path = shutil.which(self._cli)
            if cli_path is None:
                self._probe_cache = AdapterProbe(
                    available=False,
                    error=f"'{self._cli}' not found on PATH",
                )
                return self._probe_cache

            self._resolved_path = cli_path

            # Check for known-blocked paths
            for prefix in _BLOCKED_PREFIXES:
                if cli_path.startswith(prefix):
                    self._probe_cache = AdapterProbe(
                        available=False,
                        cli_path=cli_path,
                        error=f"WindowsApps sandboxed binary — cannot invoke headlessly: {cli_path}",
                    )
                    return self._probe_cache

            # Try a simple invocation to verify it runs
            test = subprocess.run(
                [cli_path, "exec", "--help"],
                capture_output=True, text=True, timeout=15,
            )
            if test.returncode != 0:
                self._probe_cache = AdapterProbe(
                    available=False,
                    cli_path=cli_path,
                    error=f"CLI returned exit code {test.returncode}: {test.stderr[:200]}",
                )
                return self._probe_cache

            # Check if --json flag is recognized
            supports_json = "--json" in (test.stdout + test.stderr)

            self._probe_cache = AdapterProbe(
                available=True,
                cli_path=cli_path,
                supports_json=supports_json,
                version=None,
            )
            return self._probe_cache

        except Exception as exc:
            self._probe_cache = AdapterProbe(
                available=False,
                error=f"Probe failed: {exc}",
            )
            return self._probe_cache

    async def run(self, task, events: list) -> AgentAdapterResult:
        """Attempt to run the Codex CLI. Returns an error result if unavailable."""
        probe = await self.probe()
        if not probe.available:
            return AgentAdapterResult(
                summary=f"Codex unavailable: {probe.error}",
                content=(
                    f"Codex CLI could not be invoked.\n"
                    f"  Path: {probe.cli_path or 'unknown'}\n"
                    f"  Error: {probe.error}\n"
                    f"\n"
                    f"The Codex adapter requires a non-sandboxed CLI binary.\n"
                    f"Fix the WindowsApps accessibility before enabling this adapter."
                ),
                proposed_status="blocked",
                needs_approval=False,
            )

        # CLI is available — invoke it
        title = getattr(task, "title", "Untitled")
        objective = getattr(task, "objective", "") or "(no objective)"

        # Validate and apply sandbox mode
        sandbox = getattr(task, "sandbox_mode", "workspace-write") or "workspace-write"
        if sandbox not in CODEX_SANDBOX_MODES:
            return AgentAdapterResult(
                summary=f"Invalid sandbox_mode: {sandbox}",
                content=(
                    f"Task sandbox_mode '{sandbox}' is not one of "
                    f"{sorted(CODEX_SANDBOX_MODES)}.\n"
                    f"This is a configuration error — the task cannot run "
                    f"until sandbox_mode is corrected."
                ),
                proposed_status="blocked",
                needs_approval=False,
            )

        prompt = (
            f"Task: {title}\n\n"
            f"Objective: {objective}\n\n"
            f"Respond concisely. If this requires code, provide the implementation.\n"
            f"End with a line: STATUS: <done|waiting_for_approval|blocked>"
        )

        try:
            args = [self._resolved_path or self._cli, "exec"]
            if probe.supports_json:
                args.append("--json")
            args.extend(["--sandbox", sandbox])
            args.append(prompt)
            result = subprocess.run(
                args, capture_output=True, text=True, timeout=300,
            )
            output = result.stdout.strip() or result.stderr.strip()
            if result.returncode != 0:
                return AgentAdapterResult(
                    summary=f"Codex exited with code {result.returncode}",
                    content=output,
                    proposed_status="queued",
                )
            return AgentAdapterResult(
                summary=output.split("\n")[0][:200] if output else f"Codex response to: {title}",
                content=output,
                proposed_status="done",
                proposed_owner="user",
            )
        except subprocess.TimeoutExpired:
            return AgentAdapterResult(
                summary="Codex timed out after 300s",
                content="The Codex CLI did not complete within the timeout.",
                proposed_status="queued",
            )
        except Exception as exc:
            return AgentAdapterResult(
                summary=f"Codex error: {exc}",
                content=str(exc),
                proposed_status="queued",
            )
