"""Advanced safe tests for collect_matches.py.

These tests focus on edge cases and date/state parsing while mocking external dependencies.
"""

from pathlib import Path
import sys
from unittest.mock import patch

import pytest

# Allow imports from repository root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import collect_matches as cm


def test_load_last_end_date_from_parse_done_entry(tmp_path: Path) -> None:
    """Collect parser should use last parse_done period end date from journal."""
    journal_file = tmp_path / "workflow_journal.log"
    journal_file.write_text(
        "\n".join(
            [
                "2026-03-05 10:00:00 | script=collect_matches | action=parse_done | period=04.03.2026..04.03.2026",
                "2026-03-05 11:00:00 | script=collect_matches | action=parse_done | period=05.03.2026..05.03.2026",
            ]
        ),
        encoding="utf-8",
    )

    with patch("collect_matches.JOURNAL_FILE", journal_file):
        try:
            result = cm.load_last_end_date("01.03.2026")
        except Exception as exc:
            pytest.fail(f"load_last_end_date crashed unexpectedly: {exc}")

    assert result == "05.03.2026"


def test_load_last_end_date_fallback_for_invalid_or_empty_data(tmp_path: Path) -> None:
    """Invalid journal content should not break parser and should return provided default."""
    journal_file = tmp_path / "workflow_journal.log"
    journal_file.write_text(
        "2026-03-05 12:00:00 | script=collect_matches | action=parse_done | period=bad..input\n",
        encoding="utf-8",
    )

    with patch("collect_matches.JOURNAL_FILE", journal_file):
        try:
            result = cm.load_last_end_date("05.03.2026")
        except Exception as exc:
            pytest.fail(f"load_last_end_date failed on invalid input: {exc}")

    assert result == "05.03.2026"


def test_build_output_file_name_for_invalid_input_returns_default() -> None:
    """Invalid dates should return the module default output file name."""
    try:
        result = cm.build_output_file_name("bad-date", "still-bad")
    except Exception as exc:
        pytest.fail(f"build_output_file_name raised unexpectedly: {exc}")

    assert result == cm.OUTPUT_FILE


def test_build_output_and_speed_profile_structure() -> None:
    """Validate expected return structures for name builder and speed profile helper."""
    with patch("collect_matches.sync_playwright") as playwright_mock:
        try:
            file_name = cm.build_output_file_name("06.03.2026", "05.03.2026", ["league-a", "league-b"])
            profile = cm.get_speed_profile(10)
        except Exception as exc:
            pytest.fail(f"collect_matches helpers raised unexpectedly: {exc}")

    assert file_name == "matches_2026-03-05_2026-03-06_2leagues.json"
    assert isinstance(profile, dict)
    assert {
        "delay_min",
        "delay_max",
        "skip_human_prob",
        "networkidle_timeout_ms",
        "batch_pages",
        "long_pause_every",
        "long_pause_prob",
        "long_pause_min",
        "long_pause_max",
    }.issubset(profile.keys())
    playwright_mock.assert_not_called()


def test_build_unique_output_path_handles_existing_versions(tmp_path: Path) -> None:
    """When base file exists, helper should select a versioned filename."""
    (tmp_path / "matches.json").write_text("[]", encoding="utf-8")
    (tmp_path / "matches_v1.json").write_text("[]", encoding="utf-8")

    try:
        output_path = cm.build_unique_output_path("matches.json", output_dir=tmp_path)
    except Exception as exc:
        pytest.fail(f"build_unique_output_path failed unexpectedly: {exc}")

    assert output_path.name == "matches_v2.json"


def test_collect_matches_wrapper_delegates_to_main() -> None:
    """Public wrapper should call main() exactly once."""
    with patch("collect_matches.main") as main_mock:
        try:
            cm.collect_matches()
        except Exception as exc:
            pytest.fail(f"collect_matches wrapper raised unexpectedly: {exc}")

    main_mock.assert_called_once_with()
