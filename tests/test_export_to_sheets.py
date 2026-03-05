"""Basic tests for export_to_sheets.py.

These tests mock Google Sheets and file interactions so real data is never modified.
"""

from datetime import date
from pathlib import Path
import sys
from unittest.mock import Mock, patch

import pytest

# Add project root so pytest can import modules from repository root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import export_to_sheets as ets


@pytest.fixture(autouse=True)
def _force_non_ci_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep tests on the standard export path regardless of CI job env vars."""
    monkeypatch.setattr(ets, "CI_MODE", False)


def test_export_to_sheets_calls_append_with_expected_args() -> None:
    """Verify export flow calls append_rows_single_batch with prepared rows."""

    expected_rows = [
        [
            123,
            "2026-03-05",
            "12:00",
            "Team A",
            "Team B",
            81,
            77,
            "Division",
            "finished",
        ]
    ]

    # Patch external dependencies (filesystem, Google API, journal writes).
    with (
        patch("export_to_sheets.setup_logging"),
        patch("export_to_sheets.start_terminal_journal", return_value=Mock()),
        patch("export_to_sheets.load_state", return_value={"last_exported_date": "2026-03-04"}),
        patch(
            "export_to_sheets.discover_new_data_files",
            return_value=[(date(2026, 3, 5), Path("dummy.json"))],
        ),
        patch(
            "export_to_sheets.collect_rows_incrementally",
            return_value=(expected_rows, date(2026, 3, 5)),
        ),
        patch("export_to_sheets.append_rows_single_batch") as append_rows_mock,
        patch("export_to_sheets.save_state"),
        patch("export_to_sheets.append_workflow_journal"),
    ):
        result = ets.export_to_sheets()

    assert result == 0
    append_rows_mock.assert_called_once_with(expected_rows)
