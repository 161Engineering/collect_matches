from __future__ import annotations

import logging
from html import escape
from typing import Any

import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_CHAT_ID_CUSTOMERS


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _build_success_message(report: dict[str, Any]) -> str:
    start_date = escape(str(report.get("start_date", "")))
    end_date = escape(str(report.get("end_date", "")))
    matches_collected = _to_int(report.get("matches_collected", 0))
    leagues_with_matches = _to_int(report.get("leagues_with_matches", 0))
    leagues_without_matches = _to_int(report.get("leagues_without_matches", 0))
    parsed_pages = _to_int(report.get("parsed_pages", 0))
    total_pages = _to_int(report.get("total_pages", 0))
    unparsed_pages = _to_int(report.get("unparsed_pages", 0))
    rows_added = _to_int(report.get("rows_added", 0))
    rows_updated = _to_int(report.get("rows_updated", 0))
    rows_skipped = _to_int(report.get("rows_skipped", 0))
    duplicates_removed = _to_int(report.get("duplicates_removed", 0))
    spreadsheet_name = escape(str(report.get("spreadsheet_name", "")))
    duration = escape(str(report.get("duration", "")))

    return (
        "📊 <b>Basketball Matches Parser Report</b>\n\n"
        "📅 <b>Period</b>\n"
        f"{start_date} → {end_date}\n\n"
        "🏀 <b>Matches</b>\n"
        f"Collected: {matches_collected}\n\n"
        "🌍 <b>Leagues</b>\n"
        f"With matches: {leagues_with_matches}\n"
        f"Without matches: {leagues_without_matches}\n\n"
        "📄 <b>Pages</b>\n"
        f"Parsed: {parsed_pages} / {total_pages}\n"
        f"Unparsed: {unparsed_pages}\n\n"
        "📤 <b>Google Sheets Export</b>\n"
        f"Added: {rows_added}\n"
        f"Updated: {rows_updated}\n"
        f"Skipped: {rows_skipped}\n"
        f"Duplicates removed: {duplicates_removed}\n\n"
        "📊 <b>Spreadsheet</b>\n"
        f"{spreadsheet_name}\n\n"
        "⏱ <b>Duration</b>\n"
        f"{duration}\n\n"
        "✅ <b>Status: SUCCESS</b>"
    )


def _build_error_message(report: dict[str, Any]) -> str:
    stage_name = escape(str(report.get("stage_name", "unknown")))
    error_message = escape(str(report.get("error_message", "unknown error")))
    return (
        "🚨 <b>Basketball Parser ERROR</b>\n\n"
        f"Stage: {stage_name}\n\n"
        "Error:\n"
        f"{error_message}"
    )


def _build_info_message(report: dict[str, Any]) -> str:
    message = escape(str(report.get("message", "Pipeline stopped by policy.")))
    return (
        "ℹ️ <b>Basketball Parser INFO</b>\n\n"
        f"{message}"
    )


def send_pipeline_report(report: dict[str, Any]) -> None:
    token = (TELEGRAM_BOT_TOKEN or "").strip()
    chat_ids_raw = [
        (TELEGRAM_CHAT_ID or "").strip(),
        (TELEGRAM_CHAT_ID_CUSTOMERS or "").strip(),
    ]
    chat_ids = list(dict.fromkeys([item for item in chat_ids_raw if item]))
    if not token or not chat_ids:
        return

    status = str(report.get("status", "")).upper().strip()
    if status == "ERROR":
        text = _build_error_message(report)
    elif status == "INFO":
        text = _build_info_message(report)
    else:
        text = _build_success_message(report)

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for chat_id in chat_ids:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }

        try:
            response = requests.post(url, data=payload, timeout=15)
            response.raise_for_status()
            parsed = response.json()
            if not parsed.get("ok"):
                logging.warning("Telegram API returned non-ok response for chat_id=%s.", chat_id)
        except Exception as exc:
            logging.warning("Telegram report send failed for chat_id=%s: %s", chat_id, exc)
