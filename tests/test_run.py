"""Tests for the repository entry script run.py.

This module validates that the main pipeline entry point can be executed.
Add more integration and edge-case checks here as project behavior grows.
"""

from pathlib import Path
import sys

import pytest

# Add project root so tests can import run.py from repository root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import run


def test_run_executes_successfully(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure run.main() executes without unhandled exceptions."""

    # Patch external side effects to keep this as a stable smoke test.
    monkeypatch.setattr(run, "ensure_project_dirs", lambda: None)
    monkeypatch.setattr(run, "collect_matches", lambda: None)
    monkeypatch.setattr(run, "export_to_sheets", lambda: 0)

    try:
        result = run.main()
    except Exception as exc:  # pragma: no cover - fail branch for clarity.
        pytest.fail(f"run.py failed with an unexpected exception: {exc}")

    assert result == 0
