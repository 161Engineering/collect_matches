"""Basic pytest template for scripts in src/.

Add new tests here for other modules as you grow the project.
"""

from pathlib import Path
import sys

# Make src/ importable for tests run from project root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from main import main
from utils import example_function


def test_main_runs_successfully() -> None:
    """Template test: verify main() runs and returns success code."""
    assert main() == 0


def test_example_function_output() -> None:
    """Template test: verify utility function expected output."""
    assert example_function() == "Example function output"
