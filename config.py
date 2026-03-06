import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
STATE_DIR = BASE_DIR / "state"
LOGS_DIR = BASE_DIR / "logs"

load_dotenv(BASE_DIR / ".env")

GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "matches_dashboard")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_CHAT_ID_CUSTOMERS = os.getenv("TELEGRAM_CHAT_ID_CUSTOMERS", "")
SITE_CALENDAR_URL = os.getenv("SITE_CALENDAR_URL", "")
SITE_NAME = os.getenv("SITE_NAME", "")


def ensure_project_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
