from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from collect_matches import collect_matches
from config import DATA_DIR, SPREADSHEET_NAME, STATE_DIR, ensure_project_dirs
from export_to_sheets import export_to_sheets, get_latest_match_date_from_sheet_with_retry
from notifications.telegram_report import send_pipeline_report


PARSE_SUMMARY_FILE = STATE_DIR / "parse_summary.json"
EXPORT_SUMMARY_FILE = STATE_DIR / "export_summary.json"


def _safe_read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _clear_run_summaries() -> None:
    for path in (PARSE_SUMMARY_FILE, EXPORT_SUMMARY_FILE):
        try:
            path.unlink(missing_ok=True)
        except Exception:
            logging.warning("Failed to clear summary file: %s", path)


def _format_duration(started_at: datetime, finished_at: datetime) -> str:
    total_seconds = max(0, int((finished_at - started_at).total_seconds()))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _build_success_report(
    parse_summary: dict[str, Any],
    export_summary: dict[str, Any],
    duration: str,
) -> dict[str, Any]:
    return {
        "start_date": str(parse_summary.get("period_start", "")),
        "end_date": str(parse_summary.get("period_end", "")),
        "matches_collected": int(parse_summary.get("matches_count", 0)),
        "leagues_with_matches": int(parse_summary.get("leagues_with_matches", 0)),
        "leagues_without_matches": int(parse_summary.get("leagues_without_matches", 0)),
        "parsed_pages": int(parse_summary.get("parsed_pages", 0)),
        "total_pages": int(parse_summary.get("total_pages", 0)),
        "unparsed_pages": int(parse_summary.get("unparsed_pages", 0)),
        "rows_added": int(export_summary.get("added_rows", 0)),
        "rows_updated": int(export_summary.get("updated_rows", 0)),
        "rows_skipped": int(export_summary.get("skipped_rows", 0)),
        "duplicates_removed": int(export_summary.get("duplicates_removed", 0)),
        "spreadsheet_name": str(export_summary.get("spreadsheet_name", SPREADSHEET_NAME)),
        "duration": duration,
        "status": "SUCCESS",
    }


def cleanup_generated_data() -> None:
    for json_path in DATA_DIR.glob("*.json"):
        try:
            json_path.unlink(missing_ok=True)
        except Exception:
            logging.warning("Failed to remove generated data file: %s", json_path)


def main() -> int:
    ensure_project_dirs()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    run_started_at = datetime.now()
    _clear_run_summaries()
    stage_name = "sheet_access_check"

    try:
        start_date_from_sheet = get_latest_match_date_from_sheet_with_retry()
        stage_name = "collect_matches"
        collect_matches(start_date_override=start_date_from_sheet)
        stage_name = "export_to_sheets"
        export_exit_code = export_to_sheets()
        if export_exit_code != 0:
            raise RuntimeError(f"export_to_sheets returned non-zero exit code: {export_exit_code}")

        parse_summary = _safe_read_json(PARSE_SUMMARY_FILE)
        export_summary = _safe_read_json(EXPORT_SUMMARY_FILE)
        duration = _format_duration(run_started_at, datetime.now())
        report = _build_success_report(parse_summary, export_summary, duration)
        send_pipeline_report(report)
        cleanup_generated_data()
        return 0
    except Exception as exc:
        if stage_name == "sheet_access_check":
            info_report = {
                "status": "INFO",
                "message": (
                    "Нет доступа к Google Sheets или не удалось определить "
                    f"дату последнего матча. Pipeline остановлен до следующего триггера. Details: {exc}"
                ),
            }
            send_pipeline_report(info_report)
            logging.warning("Pipeline skipped at stage '%s': %s", stage_name, exc)
            return 0

        error_report = {
            "status": "ERROR",
            "stage_name": stage_name,
            "error_message": str(exc),
        }
        send_pipeline_report(error_report)
        logging.exception("Pipeline failed at stage '%s': %s", stage_name, exc)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
