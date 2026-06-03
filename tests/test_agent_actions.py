"""Tests for AgentAction execution — shell, file_write, file_read, safety gates."""

import os
import tempfile
import pytest


# ── Action execution ──────────────────────────────────────────────────────────

class TestExecuteAction:

    def test_shell_echo(self):
        from src.adapters.base import AgentAction, execute_action
        action = AgentAction(type="shell", label="echo test", command="echo hello world")
        result = execute_action(action, os.getcwd())
        assert result.success is True
        assert "hello world" in result.output
        assert result.exit_code == 0

    def test_shell_failure(self):
        from src.adapters.base import AgentAction, execute_action
        action = AgentAction(type="shell", label="fail test", command="exit 1")
        result = execute_action(action, os.getcwd())
        assert result.success is False
        assert result.exit_code == 1

    def test_shell_dangerous_blocked(self):
        from src.adapters.base import AgentAction, execute_action
        action = AgentAction(type="shell", label="bad", command="rm -rf /")
        result = execute_action(action, os.getcwd())
        assert result.success is False
        assert "BLOCKED" in result.error

    def test_shell_timeout(self):
        """Subprocess has a 120s timeout — skip this test to avoid the wait."""
        pytest.skip("Timeout test requires waiting 120s")

    def test_file_write(self):
        from src.adapters.base import AgentAction, execute_action
        with tempfile.TemporaryDirectory() as tmp:
            action = AgentAction(
                type="file_write", label="write test",
                path=os.path.join(tmp, "test.txt"), content="hello file",
            )
            result = execute_action(action, tmp)
            assert result.success is True
            assert os.path.exists(os.path.join(tmp, "test.txt"))
            with open(os.path.join(tmp, "test.txt")) as f:
                assert f.read() == "hello file"

    def test_file_write_escape_blocked(self):
        from src.adapters.base import AgentAction, execute_action
        with tempfile.TemporaryDirectory() as tmp:
            action = AgentAction(
                type="file_write", label="escape",
                path=os.path.join(tmp, "..", "outside.txt"), content="bad",
            )
            result = execute_action(action, tmp)
            assert result.success is False
            assert "BLOCKED" in result.error or "escapes" in result.error.lower()

    def test_file_read(self):
        from src.adapters.base import AgentAction, execute_action
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "readme.txt")
            with open(path, "w") as f:
                f.write("read this")
            action = AgentAction(type="file_read", label="read test", path=path)
            result = execute_action(action, tmp)
            assert result.success is True
            assert "read this" in result.output

    def test_unknown_action_type(self):
        from src.adapters.base import AgentAction, execute_action
        action = AgentAction(type="nuke", label="bad")
        result = execute_action(action, os.getcwd())
        assert result.success is False
        assert "Unknown action type" in result.error


# ── Danger detection ──────────────────────────────────────────────────────────

class TestDangerDetection:

    def test_rm_rf_root(self):
        from src.adapters.base import _is_dangerous
        assert _is_dangerous("rm -rf /") is True
        assert _is_dangerous("rm -rf /  --no-preserve-root") is True

    def test_rm_rf_home(self):
        from src.adapters.base import _is_dangerous
        assert _is_dangerous("rm -rf ~") is True

    def test_dd(self):
        from src.adapters.base import _is_dangerous
        assert _is_dangerous("dd if=/dev/zero of=/dev/sda") is True

    def test_fork_bomb(self):
        from src.adapters.base import _is_dangerous
        assert _is_dangerous(":(){ :|:& };:") is True

    def test_safe_commands(self):
        from src.adapters.base import _is_dangerous
        assert _is_dangerous("echo hello") is False
        assert _is_dangerous("ls -la") is False
        assert _is_dangerous("python script.py") is False
        assert _is_dangerous("rm file.txt") is False  # single file, fine


# ── AgentAction dataclass ─────────────────────────────────────────────────────

class TestAgentActionDataclass:

    def test_shell_action(self):
        from src.adapters.base import AgentAction
        a = AgentAction(type="shell", label="list files", command="ls")
        assert a.type == "shell"
        assert a.command == "ls"

    def test_file_write_action(self):
        from src.adapters.base import AgentAction
        a = AgentAction(type="file_write", label="save", path="/tmp/x.py", content="print(1)")
        assert a.path == "/tmp/x.py"
        assert a.content == "print(1)"

    def test_defaults(self):
        from src.adapters.base import AgentAction
        a = AgentAction(type="shell")
        assert a.label == ""
        assert a.command == ""
        assert a.path == ""
