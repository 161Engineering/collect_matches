"""Basic tests for collect_matches.py.

These tests are a safe template for future parser tests and avoid real browser/API calls.
"""

from pathlib import Path
import sys
from unittest.mock import patch

# Add project root so pytest can import modules from repository root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import collect_matches as cm


def test_collect_matches_returns_expected_structure() -> None:
    """Verify collect_matches module returns expected dictionary structure."""

    # Patch external browser dependency to keep this unit test isolated.
    with patch("collect_matches.sync_playwright") as playwright_mock:
        profile = cm.get_speed_profile(10)

    expected_keys = {
        "delay_min",
        "delay_max",
        "skip_human_prob",
        "networkidle_timeout_ms",
        "batch_pages",
        "long_pause_every",
        "long_pause_prob",
        "long_pause_min",
        "long_pause_max",
    }

    assert isinstance(profile, dict)
    assert expected_keys.issubset(profile.keys())
    assert isinstance(profile["delay_min"], float)
    assert isinstance(profile["delay_max"], float)
    assert isinstance(profile["batch_pages"], int)
    playwright_mock.assert_not_called()
