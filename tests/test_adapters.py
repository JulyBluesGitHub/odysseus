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


# ── CodexAdapter SDK integration behavior ────────────────────────────────────

class TestCodexAdapterSDK:

    def test_probe_available_when_sdk_installed(self, monkeypatch):
        from src.adapters.codex import CodexAdapter
        import asyncio
        import sys
        import types

        fake_sdk = types.SimpleNamespace(version="1.2.3")
        monkeypatch.setitem(sys.modules, "openai_codex", fake_sdk)

        adapter = CodexAdapter()
        result = asyncio.run(adapter.probe())

        assert result.available is True
        assert result.version == "1.2.3"

    def test_probe_unavailable_when_sdk_missing(self):
        from src.adapters.codex import CodexAdapter
        import asyncio
        from unittest.mock import patch

        adapter = CodexAdapter()
        with patch("importlib.import_module", side_effect=ImportError):
            result = asyncio.run(adapter.probe())

        assert result.available is False
        assert result.error == "openai-codex not installed"

    def test_run_returns_result(self, monkeypatch):
        from src.adapters.codex import CodexAdapter
        import asyncio
        import sys
        import types
        from unittest.mock import MagicMock

        captured = {}

        class FakeThread:
            async def run(self, prompt):
                captured["prompt"] = prompt
                return types.SimpleNamespace(
                    final_response="Implemented the task.",
                    items=["item-1"],
                    usage={"input_tokens": 10},
                )

        class FakeAsyncCodex:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def thread_start(self, sandbox=None, cwd=None):
                captured["sandbox"] = sandbox
                captured["cwd"] = cwd
                return FakeThread()

        fake_sandbox = types.SimpleNamespace(
            read_only="ro",
            workspace_write="ww",
            full_access="fa",
        )
        fake_sdk = types.SimpleNamespace(
            version="1.2.3",
            AsyncCodex=FakeAsyncCodex,
            Sandbox=fake_sandbox,
        )
        monkeypatch.setitem(sys.modules, "openai_codex", fake_sdk)

        adapter = CodexAdapter()
        task = MagicMock()
        task.title = "Do it"
        task.objective = "Fallback objective"
        task.sandbox_mode = "workspace-write"

        result = asyncio.run(adapter.run(task, [], workspace="C:/repo"))

        assert result.content == "Implemented the task."
        assert result.summary == "Implemented the task."
        assert result.proposed_status == "done"
        assert result.metadata["usage"] == {"input_tokens": 10}
        assert captured["prompt"] == "Do it"
        assert captured["sandbox"] == "ww"
        assert captured["cwd"] == "C:/repo"

    def test_run_rejects_invalid_sandbox_mode(self):
        from src.adapters.codex import CodexAdapter
        import asyncio
        from unittest.mock import MagicMock

        adapter = CodexAdapter()
        task = MagicMock()
        task.title = "Test"
        task.objective = "Something"
        task.sandbox_mode = "bad"

        result = asyncio.run(adapter.run(task, []))

        assert result.proposed_status == "blocked"
        assert "Invalid sandbox_mode" in result.summary

    def test_run_handles_exception(self, monkeypatch):
        from src.adapters.codex import CodexAdapter
        import asyncio
        import sys
        import types
        from unittest.mock import MagicMock

        class FakeAsyncCodex:
            async def __aenter__(self):
                raise RuntimeError("sdk exploded")

            async def __aexit__(self, exc_type, exc, tb):
                return False

        fake_sdk = types.SimpleNamespace(
            version="1.2.3",
            AsyncCodex=FakeAsyncCodex,
            Sandbox=types.SimpleNamespace(
                read_only="ro",
                workspace_write="ww",
                full_access="fa",
            ),
        )
        monkeypatch.setitem(sys.modules, "openai_codex", fake_sdk)

        adapter = CodexAdapter()
        task = MagicMock()
        task.title = "Test"
        task.objective = ""
        task.sandbox_mode = "workspace-write"

        result = asyncio.run(adapter.run(task, []))

        assert result.proposed_status == "blocked"
        assert "sdk exploded" in result.summary


# ── CursorAdapter ─────────────────────────────────────────────────────────────

class TestCursorAdapter:

    def test_probe_unavailable_when_sdk_missing(self):
        from src.adapters.cursor import CursorAdapter
        import asyncio
        from unittest.mock import patch

        adapter = CursorAdapter()
        with patch("importlib.import_module", side_effect=ImportError):
            result = asyncio.run(adapter.probe())

        assert result.available is False
        assert result.error == "cursor-sdk not installed"

    def test_probe_unavailable_when_no_api_key(self, monkeypatch, tmp_path):
        from src.adapters.cursor import CursorAdapter
        import asyncio
        import sys
        import types
        from pathlib import Path

        monkeypatch.delenv("CURSOR_API_KEY", raising=False)
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setitem(sys.modules, "cursor_sdk", types.SimpleNamespace(version="0.1.0"))

        adapter = CursorAdapter()
        result = asyncio.run(adapter.probe())

        assert result.available is False
        assert result.error == "CURSOR_API_KEY not set"

    def test_probe_available_with_key(self, monkeypatch):
        from src.adapters.cursor import CursorAdapter
        import asyncio
        import sys
        import types

        monkeypatch.setenv("CURSOR_API_KEY", "test-key")
        monkeypatch.setitem(sys.modules, "cursor_sdk", types.SimpleNamespace(version="0.1.0"))

        adapter = CursorAdapter()
        result = asyncio.run(adapter.probe())

        assert result.available is True
        assert result.version == "0.1.0"

    def test_run_returns_result(self, monkeypatch):
        from src.adapters.cursor import CursorAdapter
        import asyncio
        import sys
        import types
        from unittest.mock import MagicMock

        captured = {}

        class FakeLocalAgentOptions:
            def __init__(self, cwd=None, force=False):
                captured["cwd"] = cwd
                captured["force"] = force

        class FakeAgent:
            @staticmethod
            def prompt(prompt, model=None, local=None):
                captured["prompt"] = prompt
                captured["model"] = model
                captured["local"] = local
                return "Cursor completed the task."

        fake_sdk = types.SimpleNamespace(
            version="0.1.0",
            Agent=FakeAgent,
            LocalAgentOptions=FakeLocalAgentOptions,
        )
        monkeypatch.setitem(sys.modules, "cursor_sdk", fake_sdk)

        adapter = CursorAdapter()
        task = MagicMock()
        task.title = "Cursor task"
        task.objective = "Fallback"
        task.sandbox_mode = "danger-full-access"

        result = asyncio.run(adapter.run(task, [], workspace="C:/repo"))

        assert result.content == "Cursor completed the task."
        assert result.summary == "Cursor completed the task."
        assert result.proposed_status == "done"
        assert captured["prompt"] == "Cursor task"
        assert captured["model"] == "composer-2.5"
        assert captured["cwd"] == "C:/repo"
        assert captured["force"] is True

    def test_run_rejects_invalid_sandbox_mode(self):
        from src.adapters.cursor import CursorAdapter
        import asyncio
        from unittest.mock import MagicMock

        adapter = CursorAdapter()
        task = MagicMock()
        task.title = "Test"
        task.objective = ""
        task.sandbox_mode = "invalid"

        result = asyncio.run(adapter.run(task, []))

        assert result.proposed_status == "blocked"
        assert "Invalid sandbox_mode" in result.summary

    def test_run_handles_exception(self, monkeypatch):
        from src.adapters.cursor import CursorAdapter
        import asyncio
        import sys
        import types
        from unittest.mock import MagicMock

        class FakeLocalAgentOptions:
            def __init__(self, cwd=None, force=False):
                pass

        class FakeAgent:
            @staticmethod
            def prompt(prompt, model=None, local=None):
                raise RuntimeError("cursor exploded")

        fake_sdk = types.SimpleNamespace(
            version="0.1.0",
            Agent=FakeAgent,
            LocalAgentOptions=FakeLocalAgentOptions,
        )
        monkeypatch.setitem(sys.modules, "cursor_sdk", fake_sdk)

        adapter = CursorAdapter()
        task = MagicMock()
        task.title = "Test"
        task.objective = ""
        task.sandbox_mode = "workspace-write"

        result = asyncio.run(adapter.run(task, []))

        assert result.proposed_status == "blocked"
        assert "cursor exploded" in result.summary


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
