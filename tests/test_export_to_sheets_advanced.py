"""Advanced safe tests for export_to_sheets.py.

These tests mock all external side effects (state files, journaling, and Google APIs).
"""

from datetime import date
from pathlib import Path
import sys
from unittest.mock import Mock, patch

import pytest

# Allow imports from repository root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import export_to_sheets as ets


def _dummy_rows() -> list[list[object]]:
    """Provide a small valid data batch for append/upsert tests."""
    return [[101, "2026-03-05", "10:00", "A", "B", 75, 70, "division", "finished"]]


def test_load_state_missing_file_returns_default(tmp_path: Path) -> None:
    """Missing state file should not crash and should return default structure."""
    state_dir = tmp_path / "state"
    state_file = state_dir / "export_state.json"

    with (
        patch("export_to_sheets.STATE_DIR", state_dir),
        patch("export_to_sheets.EXPORT_STATE_FILE", state_file),
    ):
        try:
            state = ets.load_state()
        except Exception as exc:
            pytest.fail(f"load_state crashed on missing file: {exc}")

    assert state == ets.DEFAULT_STATE


def test_load_state_invalid_json_returns_default(tmp_path: Path) -> None:
    """Invalid state JSON should be handled safely with fallback default."""
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    state_file = state_dir / "export_state.json"
    state_file.write_text("{bad-json}", encoding="utf-8")

    with (
        patch("export_to_sheets.STATE_DIR", state_dir),
        patch("export_to_sheets.EXPORT_STATE_FILE", state_file),
    ):
        try:
            state = ets.load_state()
        except Exception as exc:
            pytest.fail(f"load_state crashed on invalid JSON: {exc}")

    assert state == ets.DEFAULT_STATE


def test_discover_new_data_files_ignores_invalid_and_old_files(tmp_path: Path) -> None:
    """File discovery should ignore malformed names and files older than state date."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "bad_name.json").write_text("[]", encoding="utf-8")
    (data_dir / "matches_2026-03-01_2026-03-01_all-leagues.json").write_text("[]", encoding="utf-8")

    with patch("export_to_sheets.BASE_DIR", tmp_path):
        try:
            files = ets.discover_new_data_files(data_dir, "2026-03-05")
        except Exception as exc:
            pytest.fail(f"discover_new_data_files crashed unexpectedly: {exc}")

    assert files == []


def test_export_to_sheets_appends_rows_for_incremental_update() -> None:
    """When incremental rows exist, export should append rows and save state."""
    rows = _dummy_rows()
    journal = Mock()

    with (
        patch("export_to_sheets.start_terminal_journal", return_value=journal),
        patch("export_to_sheets.setup_logging"),
        patch("export_to_sheets.load_state", return_value={"last_exported_date": "2026-03-04"}),
        patch("export_to_sheets.append_workflow_journal"),
        patch("export_to_sheets.discover_new_data_files", return_value=[(date(2026, 3, 5), Path("file.json"))]),
        patch("export_to_sheets.collect_rows_incrementally", return_value=(rows, date(2026, 3, 5))),
        patch("export_to_sheets.append_rows_single_batch") as append_mock,
        patch("export_to_sheets.save_state") as save_state_mock,
    ):
        try:
            result = ets.export_to_sheets()
        except Exception as exc:
            pytest.fail(f"export_to_sheets crashed on normal incremental path: {exc}")

    assert result == 0
    append_mock.assert_called_once_with(rows)
    save_state_mock.assert_called_once_with("2026-03-05")


def test_export_to_sheets_returns_error_when_append_fails() -> None:
    """Append failure should be handled and return non-zero status."""
    journal = Mock()

    with (
        patch("export_to_sheets.start_terminal_journal", return_value=journal),
        patch("export_to_sheets.setup_logging"),
        patch("export_to_sheets.load_state", return_value={"last_exported_date": "2026-03-04"}),
        patch("export_to_sheets.append_workflow_journal"),
        patch("export_to_sheets.discover_new_data_files", return_value=[(date(2026, 3, 5), Path("file.json"))]),
        patch("export_to_sheets.collect_rows_incrementally", return_value=(_dummy_rows(), date(2026, 3, 5))),
        patch("export_to_sheets.append_rows_single_batch", side_effect=RuntimeError("append failed")),
        patch("export_to_sheets.save_state"),
    ):
        try:
            result = ets.export_to_sheets()
        except Exception as exc:
            pytest.fail(f"export_to_sheets raised instead of handling append error: {exc}")

    assert result == 1


def test_export_to_sheets_refresh_upsert_calls_append_with_rows() -> None:
    """When no new files are found but latest file exists, refresh-upsert path should append rows."""
    rows = _dummy_rows()
    journal = Mock()

    with (
        patch("export_to_sheets.start_terminal_journal", return_value=journal),
        patch("export_to_sheets.setup_logging"),
        patch("export_to_sheets.load_state", return_value={"last_exported_date": "2026-03-05"}),
        patch("export_to_sheets.append_workflow_journal"),
        patch("export_to_sheets.discover_new_data_files", return_value=[]),
        patch("export_to_sheets.discover_latest_matches_file", return_value=Path("latest.json")),
        patch("export_to_sheets.collect_rows_from_file", return_value=rows),
        patch("export_to_sheets.append_rows_single_batch") as append_mock,
    ):
        try:
            result = ets.export_to_sheets()
        except Exception as exc:
            pytest.fail(f"export_to_sheets crashed on refresh-upsert path: {exc}")

    assert result == 0
    append_mock.assert_called_once_with(rows)
