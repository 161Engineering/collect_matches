"""Integration-style pipeline test for the full project entry point.

This test executes run.main() with real module flow while mocking all external systems
(browser, network APIs, Google Sheets) and isolating file writes to a temporary directory.
"""

from datetime import date
from pathlib import Path
import sys
from unittest.mock import patch

import pytest

# Allow imports from repository root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import collect_matches
import config
import export_to_sheets
import run


def test_pipeline_integration_runs_safely(tmp_path: Path) -> None:
    """Run the real pipeline from run.py with safe mocks and local temp files only."""
    temp_data_dir = tmp_path / "data"
    temp_state_dir = tmp_path / "state"
    temp_data_dir.mkdir(parents=True, exist_ok=True)
    temp_state_dir.mkdir(parents=True, exist_ok=True)

    # Sample parsed match payload returned by mocked parser/network layer.
    sample_matches = [
        {
            "match_id": 9001,
            "date": date.today().isoformat(),
            "time": "12:00",
            "team1": "Team A",
            "team2": "Team B",
            "score1": 88,
            "score2": 80,
            "division": "integration-division",
            "status": "finished",
        }
    ]
    sample_stats = {
        "total_leagues_on_site": 1,
        "leagues_with_matches": 1,
        "leagues_without_matches": 0,
        "leagues_without_matches_list": [],
        "no_match_pages": 0,
        "unparsed_pages": 0,
    }

    try:
        with (
            # Keep run.main behavior real, but track calls for verification.
            patch.object(run, "collect_matches", wraps=collect_matches.collect_matches) as run_collect_spy,
            patch.object(run, "export_to_sheets", wraps=export_to_sheets.export_to_sheets) as run_export_spy,
            # Patch browser and parser network layer for CI-safe execution.
            patch("collect_matches.sync_playwright"),
            patch("collect_matches.fetch_calendar", return_value=(sample_matches, sample_stats)),
            # Isolate project directories to tmp_path to avoid real filesystem writes.
            patch.object(config, "DATA_DIR", temp_data_dir),
            patch.object(config, "STATE_DIR", temp_state_dir),
            patch("collect_matches.DATA_DIR", temp_data_dir),
            patch("collect_matches.JOURNAL_FILE", temp_data_dir / "workflow_journal.log"),
            patch("export_to_sheets.DATA_DIR", temp_data_dir),
            patch("export_to_sheets.STATE_DIR", temp_state_dir),
            patch("export_to_sheets.BASE_DIR", tmp_path),
            patch("export_to_sheets.JOURNAL_FILE", temp_data_dir / "workflow_journal.log"),
            patch("export_to_sheets.EXPORT_STATE_FILE", temp_state_dir / "export_state.json"),
            # Mock Google Sheets/network calls while still verifying data reaches export layer.
            patch("export_to_sheets.append_rows_single_batch") as append_rows_mock,
            patch("export_to_sheets.sync_dashboard_only"),
        ):
            result = run.main()
    except Exception as exc:
        pytest.fail(f"Pipeline integration test failed: {exc}")

    assert result == 0
    assert run_collect_spy.call_count == 1
    assert run_export_spy.call_count == 1

    # Verify data flowed from parser output into export batch payload.
    append_rows_mock.assert_called_once()
    appended_rows = append_rows_mock.call_args.args[0]
    assert isinstance(appended_rows, list)
    assert appended_rows, "Expected non-empty rows passed to append_rows_single_batch"
    assert appended_rows[0][0] == 9001
