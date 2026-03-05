import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
STATE_DIR = BASE_DIR / "state"

load_dotenv(BASE_DIR / ".env")

GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "matches_dashboard")


def ensure_project_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
