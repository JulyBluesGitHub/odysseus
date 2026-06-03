"""Tests for adapters — status extraction, prompt building, probe behavior."""

import pytest


# ── Status extraction ─────────────────────────────────────────────────────────

class TestStatusExtraction:

    def test_extract_done(self):
        from src.adapters.hermes import _extract_status
        assert _extract_status("Here is the plan.\n\n[STATUS: done]") == "done"

    def test_extract_waiting_for_approval(self):
        from src.adapters.hermes import _extract_status
        assert _extract_status("I need approval.\n[STATUS: waiting_for_approval]") == "waiting_for_approval"

    def test_extract_blocked(self):
        from src.adapters.hermes import _extract_status
        assert _extract_status("Cannot proceed.\n[STATUS: blocked]") == "blocked"

    def test_extract_queued(self):
        from src.adapters.hermes import _extract_status
        assert _extract_status("Retry needed.\n[STATUS: queued]") == "queued"

    def test_extract_last_status_wins(self):
        from src.adapters.hermes import _extract_status
        text = "[STATUS: queued]\n...\n[STATUS: done]"
        assert _extract_status(text) == "done"

    def test_extract_case_insensitive(self):
        from src.adapters.hermes import _extract_status
        assert _extract_status("[Status: DONE]") == "done"

    def test_extract_invalid_status_returns_none(self):
        from src.adapters.hermes import _extract_status
        assert _extract_status("[STATUS: exploded]") is None

    def test_extract_no_status_returns_none(self):
        from src.adapters.hermes import _extract_status
        assert _extract_status("Just a normal response.") is None

    def test_extract_whitespace_insensitive(self):
        from src.adapters.hermes import _extract_status
        assert _extract_status("[STATUS:  done  ]") == "done"


# ── Summary extraction ────────────────────────────────────────────────────────

class TestSummaryExtraction:

    def test_extract_first_line(self):
        from src.adapters.hermes import _extract_summary
        result = _extract_summary("This is the first line.\nMore text here.", "Task X")
        assert result == "This is the first line."

    def test_skips_headings(self):
        from src.adapters.hermes import _extract_summary
        result = _extract_summary("## Plan\n\nHere is the plan content.", "Task X")
        assert result == "Here is the plan content."

    def test_skips_status_line(self):
        from src.adapters.hermes import _extract_summary
        result = _extract_summary("Done.\n[STATUS: done]", "Task X")
        assert result == "Done."

    def test_long_line_truncated(self):
        from src.adapters.hermes import _extract_summary
        long_line = "x" * 200
        result = _extract_summary(long_line, "Task X")
        assert len(result) == 120
        assert result.endswith("...")

    def test_fallback_to_title(self):
        from src.adapters.hermes import _extract_summary
        result = _extract_summary("[STATUS: done]", "My Task")
        assert result == "Response to: My Task"


# ── Prompt building ───────────────────────────────────────────────────────────

class TestPromptBuilding:

    def test_basic_prompt(self):
        from src.adapters.hermes import _build_user_message
        msg = _build_user_message("Fix bug", "Fix the login bug", [])
        assert "Fix bug" in msg
        assert "Fix the login bug" in msg
        assert "STATUS" in msg

    def test_prompt_with_events(self):
        from unittest.mock import MagicMock
        from src.adapters.hermes import _build_user_message
        e1 = MagicMock()
        e1.actor = "user"
        e1.summary = "Created task"
        e2 = MagicMock()
        e2.actor = "hermes"
        e2.summary = "Analyzing"
        msg = _build_user_message("Task", "Do it", [e1, e2])
        assert "[user] Created task" in msg
        assert "[hermes] Analyzing" in msg


# ── AdapterProbe dataclass ───────────────────────────────────────────────────

class TestAdapterProbe:

    def test_available_defaults(self):
        from src.adapters.base import AdapterProbe
        p = AdapterProbe(available=True)
        assert p.available is True
        assert p.cli_path is None
        assert p.supports_json is False

    def test_unavailable_with_error(self):
        from src.adapters.base import AdapterProbe
        p = AdapterProbe(available=False, error="not found")
        assert p.available is False
        assert p.error == "not found"


# ── AgentAdapterResult dataclass ──────────────────────────────────────────────

class TestAgentAdapterResult:

    def test_basic_result(self):
        from src.adapters.base import AgentAdapterResult
        r = AgentAdapterResult(summary="Done", content="All good")
        assert r.summary == "Done"
        assert r.content == "All good"
        assert r.proposed_status is None

    def test_with_status(self):
        from src.adapters.base import AgentAdapterResult
        r = AgentAdapterResult(
            summary="Needs review",
            content="Please check",
            proposed_status="waiting_for_approval",
            needs_approval=True,
        )
        assert r.proposed_status == "waiting_for_approval"
        assert r.needs_approval is True

    def test_with_metadata(self):
        from src.adapters.base import AgentAdapterResult
        r = AgentAdapterResult(
            summary="Done",
            content="",
            metadata={"tokens": 150, "model": "qwen"},
        )
        assert r.metadata["tokens"] == 150


# ── HermesAdapter (unit tests, no Ollama) ─────────────────────────────────────

class TestHermesAdapterUnit:

    def test_probe_returns_unavailable_when_no_ollama(self):
        """Without a running Ollama, probe should return unavailable."""
        from src.adapters.hermes import HermesAdapter
        import asyncio
        adapter = HermesAdapter(ollama_url="http://127.0.0.1:19999")
        result = asyncio.run(adapter.probe())
        assert result.available is False
        assert result.error is not None

    def test_default_url(self):
        from src.adapters.hermes import HermesAdapter
        adapter = HermesAdapter()
        assert "11434" in adapter._ollama_url

    def test_custom_model(self):
        from src.adapters.hermes import HermesAdapter
        adapter = HermesAdapter(model="mistral:7b")
        assert adapter._model == "mistral:7b"


# ── CodexAdapter (unit tests, probe only) ─────────────────────────────────────

class TestCodexAdapterUnit:

    def test_probe_not_found(self):
        """Probe should fail when codex is not on PATH."""
        from src.adapters.codex import CodexAdapter
        import asyncio
        adapter = CodexAdapter(cli_command="nonexistent_codex_binary_xyz")
        result = asyncio.run(adapter.probe())
        assert result.available is False

    def test_probe_is_cached(self):
        """Second probe call should return cached result."""
        from src.adapters.codex import CodexAdapter
        import asyncio
        adapter = CodexAdapter(cli_command="nonexistent_codex_binary_xyz")
        r1 = asyncio.run(adapter.probe())
        r2 = asyncio.run(adapter.probe())
        assert r1 is r2  # same object, cached

    def test_run_when_unavailable_returns_error_result(self):
        """When probe fails, run() returns a blocked result with the error."""
        from src.adapters.codex import CodexAdapter
        import asyncio
        from unittest.mock import MagicMock
        adapter = CodexAdapter(cli_command="nonexistent_codex_binary_xyz")
        # Force probe to set the cache
        asyncio.run(adapter.probe())
        task = MagicMock()
        task.title = "Test"
        task.objective = "Do something"
        result = asyncio.run(adapter.run(task, []))
        assert result.proposed_status == "blocked"
        assert "unavailable" in result.summary.lower() or "not found" in result.summary.lower()

    def test_run_rejects_invalid_sandbox_mode(self):
        """When task.sandbox_mode is invalid, run() returns blocked before subprocess."""
        from src.adapters.codex import CodexAdapter
        import asyncio
        from unittest.mock import MagicMock, patch
        adapter = CodexAdapter(cli_command="codex")

        # Make probe return available so we reach sandbox validation
        from src.adapters.base import AdapterProbe
        adapter._probe_cache = AdapterProbe(
            available=True, cli_path="/fake/codex", supports_json=True,
        )

        task = MagicMock()
        task.title = "Test"
        task.objective = "Something"
        task.sandbox_mode = "bananas"
        result = asyncio.run(adapter.run(task, []))
        assert result.proposed_status == "blocked"
        assert "Invalid sandbox_mode" in result.summary
        assert "bananas" in result.summary


# ── MockAdapter ───────────────────────────────────────────────────────────────

class TestMockAdapter:

    def test_probe_always_available(self):
        from src.adapters.mock import MockAdapter
        import asyncio
        adapter = MockAdapter()
        result = asyncio.run(adapter.probe())
        assert result.available is True

    def test_run_echoes_task(self):
        from src.adapters.mock import MockAdapter
        import asyncio
        from unittest.mock import MagicMock
        adapter = MockAdapter()
        task = MagicMock()
        task.title = "Hello World"
        task.objective = "Say hi"
        result = asyncio.run(adapter.run(task, []))
        assert "Hello World" in result.content
        assert "Say hi" in result.content
        assert result.proposed_status == "done"
        assert result.proposed_owner == "user"
