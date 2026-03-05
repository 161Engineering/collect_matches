import json
import logging
import os
import re
import socket
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import gspread
import urllib3
from google.auth.exceptions import TransportError
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
from gspread.utils import ValueInputOption
from gspread.worksheet import Worksheet
from google.oauth2.service_account import Credentials
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config import DATA_DIR, GOOGLE_CREDS_PATH, SPREADSHEET_NAME, STATE_DIR


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "match_id",
    "date",
    "time",
    "team1",
    "team2",
    "score1",
    "score2",
    "division",
    "status",
]

WORKSHEET_TITLE = "matches"
ANALYSIS_SHEET_TITLE = "analysis"
DATE_FILE_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})\.json$")
MATCHES_RANGE_FILE_PATTERN = re.compile(r"^matches_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_.*\.json$")
DEFAULT_STATE = {"last_exported_date": "0001-01-01"}

BASE_DIR = Path(__file__).resolve().parent
JOURNAL_FILE = DATA_DIR / "workflow_journal.log"
EXPORT_STATE_FILE = STATE_DIR / "export_state.json"

CI_MODE = os.getenv("CI_MODE", "").strip().lower() == "true"
if CI_MODE:
    DATA_DIR = Path("ci_output")
    STATE_DIR = DATA_DIR / "state"
    JOURNAL_FILE = DATA_DIR / "workflow_journal.log"
    EXPORT_STATE_FILE = STATE_DIR / "export_state.json"


class TerminalJournalTee:
    def __init__(self, original_stream, log_file, script_name: str, stream_name: str) -> None:
        self.original_stream = original_stream
        self.log_file = log_file
        self.script_name = script_name
        self.stream_name = stream_name
        self.buffer = ""

    def write(self, text: str) -> int:
        if not text:
            return 0

        self.original_stream.write(text)
        self.buffer += text

        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            level = detect_stream_level(line)
            self.log_file.write(
                f"{timestamp} | script={self.script_name} | stream={self.stream_name} | level={level} | {line}\n"
            )

        self.log_file.flush()
        return len(text)

    def flush(self) -> None:
        if self.buffer:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            level = detect_stream_level(self.buffer)
            self.log_file.write(
                f"{timestamp} | script={self.script_name} | stream={self.stream_name} | level={level} | {self.buffer}\n"
            )
            self.buffer = ""
        self.log_file.flush()
        self.original_stream.flush()

    def __getattr__(self, name: str):
        return getattr(self.original_stream, name)


def start_terminal_journal(script_name: str):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log_file = JOURNAL_FILE.open("a", encoding="utf-8")
    run_mark = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_file.write(f"{run_mark} | script={script_name} | action=run_open\n")
    log_file.flush()

    sys.stdout = TerminalJournalTee(sys.stdout, log_file, script_name, "stdout")
    sys.stderr = TerminalJournalTee(sys.stderr, log_file, script_name, "stderr")
    return log_file


def detect_stream_level(line: str) -> str:
    upper_line = (line or "").upper()
    if "ERROR" in upper_line:
        return "ERROR"
    if "WARN" in upper_line or "WARNING" in upper_line:
        return "WARN"
    return "INFO"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def append_workflow_journal(action: str, details: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with JOURNAL_FILE.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{now} | script=export_to_sheets | action={action} | {details}\n")


def parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except Exception:
        return None


def load_state() -> dict[str, str]:
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    if not EXPORT_STATE_FILE.exists():
        return DEFAULT_STATE.copy()

    try:
        payload = json.loads(EXPORT_STATE_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        logging.warning("Не удалось прочитать state файл %s: %s", EXPORT_STATE_FILE, exc)
        return DEFAULT_STATE.copy()

    if not isinstance(payload, dict):
        logging.warning("Некорректный формат state файла %s", EXPORT_STATE_FILE)
        return DEFAULT_STATE.copy()

    value = str(payload.get("last_exported_date", "")).strip()
    if parse_iso_date(value) is None:
        logging.warning("Некорректная дата в state файле: %s", value)
        return DEFAULT_STATE.copy()

    return {"last_exported_date": value}


def save_state(last_exported_date: str) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_STATE_FILE.write_text(
        json.dumps({"last_exported_date": last_exported_date}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logging.info("State обновлен в %s: last_exported_date=%s", EXPORT_STATE_FILE, last_exported_date)
    append_workflow_journal("state_update", f"last_exported_date={last_exported_date}")


def discover_new_data_files(data_dir: Path, last_exported_date: str) -> list[tuple[date, Path]]:
    candidate_dirs: list[Path] = []

    if data_dir.exists() and data_dir.is_dir():
        candidate_dirs.append(data_dir)
    else:
        logging.warning("Папка с данными не найдена: %s", data_dir)

    if BASE_DIR not in candidate_dirs:
        candidate_dirs.append(BASE_DIR)

    last_date = parse_iso_date(last_exported_date)
    if last_date is None:
        last_date = parse_iso_date(DEFAULT_STATE["last_exported_date"]) or date.min

    result: list[tuple[date, Path]] = []
    range_result: list[tuple[date, Path]] = []

    seen_paths: set[Path] = set()

    for source_dir in candidate_dirs:
        for item in source_dir.iterdir():
            if not item.is_file() or item.suffix.lower() != ".json":
                continue

            file_date: date | None = None

            day_match = DATE_FILE_PATTERN.match(item.name)
            if day_match:
                file_date = parse_iso_date(day_match.group(1))
            else:
                range_match = MATCHES_RANGE_FILE_PATTERN.match(item.name)
                if range_match:
                    file_date = parse_iso_date(range_match.group(2))

            if file_date is None or file_date <= last_date:
                continue

            resolved = item.resolve()
            if resolved in seen_paths:
                continue

            seen_paths.add(resolved)
            pair = (file_date, item)
            result.append(pair)

            if MATCHES_RANGE_FILE_PATTERN.match(item.name):
                range_result.append(pair)

    result.sort(key=lambda pair: pair[0])

    if range_result:
        latest_range = max(range_result, key=lambda pair: (pair[0], pair[1].stat().st_mtime))
        logging.info("Выбран самый свежий файл matches_* для экспорта: %s", latest_range[1].name)
        return [latest_range]

    return result


def discover_latest_matches_file() -> Path | None:
    candidate_dirs: list[Path] = []
    if DATA_DIR.exists() and DATA_DIR.is_dir():
        candidate_dirs.append(DATA_DIR)
    if BASE_DIR not in candidate_dirs:
        candidate_dirs.append(BASE_DIR)

    latest_pair: tuple[date, float, Path] | None = None

    for source_dir in candidate_dirs:
        for item in source_dir.iterdir():
            if not item.is_file() or item.suffix.lower() != ".json":
                continue

            range_match = MATCHES_RANGE_FILE_PATTERN.match(item.name)
            if not range_match:
                continue

            end_date = parse_iso_date(range_match.group(2))
            if end_date is None:
                continue

            pair = (end_date, item.stat().st_mtime, item)
            if latest_pair is None or pair > latest_pair:
                latest_pair = pair

    return latest_pair[2] if latest_pair else None


def _to_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def match_to_row(match: dict[str, Any]) -> list[Any] | None:
    match_id = _to_int(match.get("match_id"))
    if match_id is None:
        return None

    score1 = _to_int(match.get("score1"))
    score2 = _to_int(match.get("score2"))

    return [
        match_id,
        str(match.get("date", "")).strip(),
        str(match.get("time", "")).strip(),
        str(match.get("team1", "")).strip(),
        str(match.get("team2", "")).strip(),
        score1 if score1 is not None else "",
        score2 if score2 is not None else "",
        str(match.get("division", "")).strip(),
        str(match.get("status", "")).strip(),
    ]


def collect_rows_incrementally(new_files: list[tuple[date, Path]]) -> tuple[list[list[Any]], date | None]:
    all_rows: list[list[Any]] = []
    last_contiguous_processed_date: date | None = None

    for file_date, file_path in new_files:
        logging.info("Чтение файла: %s", file_path.name)

        try:
            content = file_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            logging.error("Не удалось прочитать файл %s: %s", file_path, exc)
            break

        if not content:
            logging.warning("Пустой файл: %s", file_path.name)
            last_contiguous_processed_date = file_date
            continue

        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            logging.error("Битый JSON в файле %s: %s", file_path.name, exc)
            break

        if not isinstance(payload, list):
            logging.error("Ожидался массив матчей в %s", file_path.name)
            break

        valid_rows_in_file = 0

        for index, item in enumerate(payload, start=1):
            if not isinstance(item, dict):
                logging.warning("Пропуск записи #%s в %s: ожидается объект", index, file_path.name)
                continue

            row = match_to_row(item)
            if row is None:
                logging.warning("Пропуск записи #%s в %s: некорректный match_id", index, file_path.name)
                continue

            all_rows.append(row)
            valid_rows_in_file += 1

        logging.info("Файл %s: валидных матчей %s", file_path.name, valid_rows_in_file)
        last_contiguous_processed_date = file_date

    return all_rows, last_contiguous_processed_date


def collect_rows_from_file(file_path: Path) -> list[list[Any]]:
    try:
        content = file_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        logging.error("Не удалось прочитать файл %s: %s", file_path, exc)
        return []

    if not content:
        return []

    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        logging.error("Битый JSON в файле %s: %s", file_path.name, exc)
        return []

    if not isinstance(payload, list):
        logging.error("Ожидался массив матчей в %s", file_path.name)
        return []

    rows: list[list[Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        row = match_to_row(item)
        if row is not None:
            rows.append(row)

    return rows


def build_gspread_client(creds_path: str) -> gspread.Client:
    credentials = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return gspread.authorize(credentials)


def open_or_create_spreadsheet(client: gspread.Client) -> gspread.Spreadsheet:
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    spreadsheet_url = os.getenv("SPREADSHEET_URL", "").strip()
    spreadsheet_name = SPREADSHEET_NAME.strip()

    if spreadsheet_id:
        logging.info("Открытие Google Sheets по SPREADSHEET_ID")
        return client.open_by_key(spreadsheet_id)

    if spreadsheet_url:
        logging.info("Открытие Google Sheets по SPREADSHEET_URL")
        return client.open_by_url(spreadsheet_url)

    target_name = spreadsheet_name
    try:
        return client.open(target_name)
    except SpreadsheetNotFound:
        pass

    logging.warning("Таблица '%s' не найдена. Создаю новую.", target_name)
    return client.create(target_name)


def get_matches_worksheet(spreadsheet: gspread.Spreadsheet):
    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_TITLE)
    except WorksheetNotFound:
        logging.info("Лист '%s' не найден, создаю новый", WORKSHEET_TITLE)
        worksheet = spreadsheet.add_worksheet(title=WORKSHEET_TITLE, rows=1000, cols=len(HEADERS))
    return worksheet


def get_or_create_analysis_worksheet(spreadsheet: gspread.Spreadsheet) -> Worksheet:
    try:
        worksheet = spreadsheet.worksheet(ANALYSIS_SHEET_TITLE)
    except WorksheetNotFound:
        logging.info("Лист '%s' не найден, создаю новый", ANALYSIS_SHEET_TITLE)
        worksheet = spreadsheet.add_worksheet(title=ANALYSIS_SHEET_TITLE, rows=300, cols=40)

    if worksheet.row_count < 300 or worksheet.col_count < 40:
        worksheet.resize(rows=max(300, worksheet.row_count), cols=max(40, worksheet.col_count))

    return worksheet


def _grid_range(sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int) -> dict[str, int]:
    return {
        "sheetId": sheet_id,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col,
    }


def write_analysis_layout(analysis_ws: Worksheet, team_a: str = "", team_b: str = "") -> None:
    analysis_ws.batch_clear([
        "A1:D30",
        "Z1:AD40",
    ])

    analysis_ws.update(
        range_name="A1:B3",
        values=[
            ["Выбор команд", ""],
            ["Команда 1", team_a],
            ["Команда 2", team_b],
        ],
        value_input_option=ValueInputOption.user_entered,
    )

    analysis_ws.update(
        range_name="A5:B6",
        values=[
            ["Фаворит по H2H:", '=IF(B11>C11,B2,IF(B11<C11,B3,"Равные шансы"))'],
            ["Фаворит по сезону:", '=IF((B16+B17)>(C16+C17),B2,IF((B16+B17)<(C16+C17),B3,"Равные шансы"))'],
        ],
        value_input_option=ValueInputOption.user_entered,
    )

    analysis_ws.update(
        range_name="A8:C20",
        values=[
            ["Показатель", "=B2", "=B3"],
            ["H2H Игр", "=SUMPRODUCT(((matches!$D$2:$D$200000=$B$2)*(matches!$E$2:$E$200000=$B$3))+((matches!$D$2:$D$200000=$B$3)*(matches!$E$2:$E$200000=$B$2)))", "=B9"],
            ["H2H Победы", "=SUMPRODUCT(((matches!$D$2:$D$200000=$B$2)*(matches!$E$2:$E$200000=$B$3)*(IFERROR(matches!$F$2:$F$200000*1,0)>IFERROR(matches!$G$2:$G$200000*1,0)))+((matches!$D$2:$D$200000=$B$3)*(matches!$E$2:$E$200000=$B$2)*(IFERROR(matches!$G$2:$G$200000*1,0)>IFERROR(matches!$F$2:$F$200000*1,0))))", "=SUMPRODUCT(((matches!$D$2:$D$200000=$B$3)*(matches!$E$2:$E$200000=$B$2)*(IFERROR(matches!$F$2:$F$200000*1,0)>IFERROR(matches!$G$2:$G$200000*1,0)))+((matches!$D$2:$D$200000=$B$2)*(matches!$E$2:$E$200000=$B$3)*(IFERROR(matches!$G$2:$G$200000*1,0)>IFERROR(matches!$F$2:$F$200000*1,0))))"],
            ["H2H % побед", "=IFERROR(B10/B9,0)", "=IFERROR(C10/C9,0)"],
            ["Средние очки в H2H", "=IF(B9=0,0,ROUND((SUMPRODUCT((matches!$D$2:$D$200000=$B$2)*(matches!$E$2:$E$200000=$B$3)*matches!$F$2:$F$200000)+SUMPRODUCT((matches!$D$2:$D$200000=$B$3)*(matches!$E$2:$E$200000=$B$2)*matches!$G$2:$G$200000))/B9,0))", "=IF(C9=0,0,ROUND((SUMPRODUCT((matches!$D$2:$D$200000=$B$2)*(matches!$E$2:$E$200000=$B$3)*matches!$G$2:$G$200000)+SUMPRODUCT((matches!$D$2:$D$200000=$B$3)*(matches!$E$2:$E$200000=$B$2)*matches!$F$2:$F$200000))/C9,0))"],
            ["Средний тотал в H2H", "=IF(B9=0,0,ROUND(SUMPRODUCT(((matches!$D$2:$D$200000=$B$2)*(matches!$E$2:$E$200000=$B$3)+(matches!$D$2:$D$200000=$B$3)*(matches!$E$2:$E$200000=$B$2))*(matches!$F$2:$F$200000+matches!$G$2:$G$200000))/B9,0))", "=B13"],
            ["", "", ""],
            ["Всего игр", "=COUNTIF(matches!$D$2:$D$200000,$B$2)+COUNTIF(matches!$E$2:$E$200000,$B$2)", "=COUNTIF(matches!$D$2:$D$200000,$B$3)+COUNTIF(matches!$E$2:$E$200000,$B$3)"],
            ["Win Rate", "=IFERROR((SUMPRODUCT((matches!$D$2:$D$200000=$B$2)*(IFERROR(matches!$F$2:$F$200000*1,0)>IFERROR(matches!$G$2:$G$200000*1,0)))+SUMPRODUCT((matches!$E$2:$E$200000=$B$2)*(IFERROR(matches!$G$2:$G$200000*1,0)>IFERROR(matches!$F$2:$F$200000*1,0))))/B15,0)", "=IFERROR((SUMPRODUCT((matches!$D$2:$D$200000=$B$3)*(IFERROR(matches!$F$2:$F$200000*1,0)>IFERROR(matches!$G$2:$G$200000*1,0)))+SUMPRODUCT((matches!$E$2:$E$200000=$B$3)*(IFERROR(matches!$G$2:$G$200000*1,0)>IFERROR(matches!$F$2:$F$200000*1,0))))/C15,0)"],
            ["Point Differential", "=IF(B15=0,0,ROUND((SUMPRODUCT((matches!$D$2:$D$200000=$B$2)*matches!$F$2:$F$200000)+SUMPRODUCT((matches!$E$2:$E$200000=$B$2)*matches!$G$2:$G$200000)-SUMPRODUCT((matches!$D$2:$D$200000=$B$2)*matches!$G$2:$G$200000)-SUMPRODUCT((matches!$E$2:$E$200000=$B$2)*matches!$F$2:$F$200000))/B15,1))", "=IF(C15=0,0,ROUND((SUMPRODUCT((matches!$D$2:$D$200000=$B$3)*matches!$F$2:$F$200000)+SUMPRODUCT((matches!$E$2:$E$200000=$B$3)*matches!$G$2:$G$200000)-SUMPRODUCT((matches!$D$2:$D$200000=$B$3)*matches!$G$2:$G$200000)-SUMPRODUCT((matches!$E$2:$E$200000=$B$3)*matches!$F$2:$F$200000))/C15,1))"],
            ["Last 5 Wins", "=SUM($AB$23:$AB$27)", "=SUM($AD$23:$AD$27)"],
            ["Season Avg Points Scored (All Games)", "=IFERROR(ROUND(AVERAGE(ARRAYFORMULA(FILTER(IF(matches!D2:D=B2,IFERROR(matches!F2:F*1,0),IFERROR(matches!G2:G*1,0)),(matches!D2:D=B2)+(matches!E2:E=B2)))),0),0)", "=IFERROR(ROUND(AVERAGE(ARRAYFORMULA(FILTER(IF(matches!D2:D=B3,IFERROR(matches!F2:F*1,0),IFERROR(matches!G2:G*1,0)),(matches!D2:D=B3)+(matches!E2:E=B3)))),0),0)"],
            ["Season Average Total (All Games)", "=IFERROR(ROUND(AVERAGE(ARRAYFORMULA(FILTER(IFERROR(matches!F2:F*1,0)+IFERROR(matches!G2:G*1,0),(matches!D2:D=B2)+(matches!E2:E=B2)))),0),0)", "=IFERROR(ROUND(AVERAGE(ARRAYFORMULA(FILTER(IFERROR(matches!F2:F*1,0)+IFERROR(matches!G2:G*1,0),(matches!D2:D=B3)+(matches!E2:E=B3)))),0),0)"],
        ],
        value_input_option=ValueInputOption.user_entered,
    )

    analysis_ws.update(
        range_name="A22:A27",
        values=[
            ["Пояснения к метрикам"],
            ["Win Rate — процент побед команды от общего числа сыгранных матчей."],
            ["Point Differential — средняя разница между набранными и пропущенными очками за игру."],
            ["Last 5 Wins — количество побед в последних пяти матчах."],
            ["Season Avg Points Scored (All Games) — среднее количество набранных очков команды (свои очки) по всем матчам сезона."],
            ["Season Average Total (All Games) — средний суммарный тотал (score1 + score2) по всем матчам команды за сезон."],
        ],
        value_input_option=ValueInputOption.user_entered,
    )

    analysis_ws.update(
        range_name="Z1",
        values=[["=SORT(UNIQUE(FILTER(TOCOL(matches!D2:E,1),TOCOL(matches!D2:E,1)<>\"\")))"]],
        value_input_option=ValueInputOption.user_entered,
    )

    analysis_ws.update(
        range_name="AA22:AD22",
        values=[["last5a_row", "last5a_win", "last5b_row", "last5b_win"]],
        value_input_option=ValueInputOption.user_entered,
    )

    for row in range(23, 28):
        if row == 23:
            a_formula = "=IF($B$2=\"\",\"\",IFERROR(MAX(FILTER(ROW(matches!$A$2:$A),(((matches!$D$2:$D=$B$2)+(matches!$E$2:$E=$B$2))>0)*(matches!$A$2:$A<>\"\"))),\"\"))"
            c_formula = "=IF($B$3=\"\",\"\",IFERROR(MAX(FILTER(ROW(matches!$A$2:$A),(((matches!$D$2:$D=$B$3)+(matches!$E$2:$E=$B$3))>0)*(matches!$A$2:$A<>\"\"))),\"\"))"
        else:
            prev_row = row - 1
            a_formula = (
                f"=IF($B$2=\"\",\"\",IFERROR(MAX(FILTER(ROW(matches!$A$2:$A),(((matches!$D$2:$D=$B$2)+(matches!$E$2:$E=$B$2))>0)"
                f"*(matches!$A$2:$A<>\"\")*(ROW(matches!$A$2:$A)<$AA${prev_row}))),\"\"))"
            )
            c_formula = (
                f"=IF($B$3=\"\",\"\",IFERROR(MAX(FILTER(ROW(matches!$A$2:$A),(((matches!$D$2:$D=$B$3)+(matches!$E$2:$E=$B$3))>0)"
                f"*(matches!$A$2:$A<>\"\")*(ROW(matches!$A$2:$A)<$AC${prev_row}))),\"\"))"
            )

        b_formula = (
            f"=IF($AA{row}=\"\",0,IF(INDEX(matches!$D:$D,$AA{row})=$B$2,"
            f"--(IFERROR(INDEX(matches!$F:$F,$AA{row})*1,0)>IFERROR(INDEX(matches!$G:$G,$AA{row})*1,0)),"
            f"--(IFERROR(INDEX(matches!$G:$G,$AA{row})*1,0)>IFERROR(INDEX(matches!$F:$F,$AA{row})*1,0))))"
        )
        d_formula = (
            f"=IF($AC{row}=\"\",0,IF(INDEX(matches!$D:$D,$AC{row})=$B$3,"
            f"--(IFERROR(INDEX(matches!$F:$F,$AC{row})*1,0)>IFERROR(INDEX(matches!$G:$G,$AC{row})*1,0)),"
            f"--(IFERROR(INDEX(matches!$G:$G,$AC{row})*1,0)>IFERROR(INDEX(matches!$F:$F,$AC{row})*1,0))))"
        )

        analysis_ws.update(
            range_name=f"AA{row}:AD{row}",
            values=[[a_formula, b_formula, c_formula, d_formula]],
            value_input_option=ValueInputOption.user_entered,
        )


def estimate_analysis_column_widths(matches_profile_rows: list[list[str]]) -> tuple[int, int]:
    metric_labels = [
        "Показатель",
        "H2H Игр",
        "H2H Победы",
        "H2H % побед",
        "Средние очки в H2H",
        "Средний тотал в H2H",
        "Всего игр",
        "Win Rate",
        "Point Differential",
        "Last 5 Wins",
        "Season Avg Points Scored (All Games)",
        "Season Average Total (All Games)",
    ]
    max_metric_len = max(len(label) for label in metric_labels)
    col_a_px = max(190, min(250, int(max_metric_len * 5.8 + 28)))

    max_name_len = 0
    for row in matches_profile_rows:
        team1 = (row[0] if len(row) > 0 else "").strip()
        team2 = (row[1] if len(row) > 1 else "").strip()
        division = (row[4] if len(row) > 4 else "").strip()
        max_name_len = max(max_name_len, len(team1), len(team2), len(division))

    col_bc_px = max(190, min(320, int(max_name_len * 7.6 + 34)))
    return col_a_px, col_bc_px


def style_dashboard(
    spreadsheet: gspread.Spreadsheet,
    analysis_ws: Worksheet,
    matches_ws: Worksheet,
    col_a_px: int,
    col_bc_px: int,
) -> None:
    sheet_id = analysis_ws.id

    analysis_ws.format("A1", {"textFormat": {"bold": True, "fontSize": 13}})
    analysis_ws.format("A2:A3", {"textFormat": {"bold": True, "fontSize": 11}})
    analysis_ws.format("B2:B3", {
        "textFormat": {"bold": True, "fontSize": 12},
        "horizontalAlignment": "CENTER",
        "backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.8},
    })

    thick_border = {
        "top": {"style": "SOLID_THICK"},
        "bottom": {"style": "SOLID_THICK"},
        "left": {"style": "SOLID_THICK"},
        "right": {"style": "SOLID_THICK"},
    }
    analysis_ws.format("A1:B3", {"borders": thick_border})

    analysis_ws.format("A8:C8", {
        "textFormat": {"bold": True, "fontSize": 11},
        "horizontalAlignment": "CENTER",
    })

    border_style = {
        "top": {"style": "SOLID", "color": {"red": 0.85, "green": 0.85, "blue": 0.85}},
        "bottom": {"style": "SOLID", "color": {"red": 0.85, "green": 0.85, "blue": 0.85}},
        "left": {"style": "SOLID", "color": {"red": 0.85, "green": 0.85, "blue": 0.85}},
        "right": {"style": "SOLID", "color": {"red": 0.85, "green": 0.85, "blue": 0.85}},
    }
    analysis_ws.format("A8:C20", {"borders": border_style})

    analysis_ws.format("A9:C13", {"backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}})
    analysis_ws.format("A15:C20", {"backgroundColor": {"red": 0.87, "green": 0.92, "blue": 0.97}})
    analysis_ws.format("B9:C20", {"horizontalAlignment": "CENTER"})
    analysis_ws.format("A8:A20", {"textFormat": {"foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}})

    analysis_ws.format("B11:C11", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})
    analysis_ws.format("B16:C16", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})
    analysis_ws.format("B17:C17", {"numberFormat": {"type": "NUMBER", "pattern": "0.0"}})
    analysis_ws.format("B19:C20", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})
    analysis_ws.format("B12:C13", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})

    analysis_ws.format("A22", {"textFormat": {"bold": True, "fontSize": 10, "foregroundColor": {"red": 0.4, "green": 0.4, "blue": 0.4}}})
    analysis_ws.format("A23:A27", {"textFormat": {"fontSize": 9, "foregroundColor": {"red": 0.4, "green": 0.4, "blue": 0.4}}})

    requests = [
        {
            "setDataValidation": {
                "range": _grid_range(sheet_id, 1, 3, 1, 2),
                "rule": {
                    "condition": {
                        "type": "ONE_OF_RANGE",
                        "values": [{"userEnteredValue": "='analysis'!$Z$1:$Z"}],
                    },
                    "strict": False,
                    "showCustomUi": True,
                },
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 25,
                    "endIndex": 30,
                },
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": col_a_px},
                "fields": "pixelSize",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,
                    "endIndex": 3,
                },
                "properties": {"pixelSize": col_bc_px},
                "fields": "pixelSize",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 8}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "index": 0},
                "fields": "index",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {"sheetId": matches_ws.id, "index": 1},
                "fields": "index",
            }
        },
    ]
    spreadsheet.batch_update({"requests": requests})


def ensure_spreadsheet_locale(spreadsheet: gspread.Spreadsheet) -> None:
    metadata = spreadsheet.fetch_sheet_metadata()
    properties = metadata.get("properties", {})
    current_locale = str(properties.get("locale", "")).strip()
    if current_locale.lower() == "en_us":
        return

    spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateSpreadsheetProperties": {
                        "properties": {"locale": "en_US"},
                        "fields": "locale",
                    }
                }
            ]
        }
    )
    logging.info("Локаль таблицы обновлена: %s -> en_US", current_locale or "<empty>")


def apply_dashboard_conditional_formatting(spreadsheet: gspread.Spreadsheet, analysis_ws: Worksheet) -> None:
    metadata = spreadsheet.fetch_sheet_metadata()
    sheet_meta = next((s for s in metadata.get("sheets", []) if s.get("properties", {}).get("sheetId") == analysis_ws.id), None)
    if sheet_meta is None:
        return

    existing = sheet_meta.get("conditionalFormats", [])
    clear_requests = [
        {"deleteConditionalFormatRule": {"sheetId": analysis_ws.id, "index": idx}}
        for idx in range(len(existing) - 1, -1, -1)
    ]
    if clear_requests:
        spreadsheet.batch_update({"requests": clear_requests})

    green = {"red": 0.78, "green": 0.94, "blue": 0.81}
    red = {"red": 1.0, "green": 0.78, "blue": 0.78}

    rules = [
        ("$B$11>$C$11", _grid_range(analysis_ws.id, 10, 11, 1, 2), green),
        ("$B$11<$C$11", _grid_range(analysis_ws.id, 10, 11, 1, 2), red),
        ("$C$11>$B$11", _grid_range(analysis_ws.id, 10, 11, 2, 3), green),
        ("$C$11<$B$11", _grid_range(analysis_ws.id, 10, 11, 2, 3), red),
        ("$B$17>0", _grid_range(analysis_ws.id, 16, 17, 1, 2), green),
        ("$B$17<0", _grid_range(analysis_ws.id, 16, 17, 1, 2), red),
        ("$C$17>0", _grid_range(analysis_ws.id, 16, 17, 2, 3), green),
        ("$C$17<0", _grid_range(analysis_ws.id, 16, 17, 2, 3), red),
    ]

    add_requests = []
    for formula, target_range, color in rules:
        formula_value = formula if formula.startswith("=") else f"={formula}"
        add_requests.append(
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [target_range],
                        "booleanRule": {
                            "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula_value}]},
                            "format": {"backgroundColor": color},
                        },
                    },
                    "index": 0,
                }
            }
        )

    spreadsheet.batch_update({"requests": add_requests})


def protect_dashboard_sheet(spreadsheet: gspread.Spreadsheet, analysis_ws: Worksheet) -> None:
    metadata = spreadsheet.fetch_sheet_metadata()
    sheet_meta = next((s for s in metadata.get("sheets", []) if s.get("properties", {}).get("sheetId") == analysis_ws.id), None)
    if sheet_meta is None:
        return

    delete_requests = []
    for protected in sheet_meta.get("protectedRanges", []):
        protected_id = protected.get("protectedRangeId")
        if protected_id is not None:
            delete_requests.append({"deleteProtectedRange": {"protectedRangeId": protected_id}})

    add_request = {
        "addProtectedRange": {
            "protectedRange": {
                "range": {"sheetId": analysis_ws.id},
                "description": "analysis_lock",
                "warningOnly": False,
                "unprotectedRanges": [_grid_range(analysis_ws.id, 1, 3, 1, 2)],
            }
        }
    }

    spreadsheet.batch_update({"requests": delete_requests + [add_request]})


def sync_analysis_dashboard(spreadsheet: gspread.Spreadsheet, matches_ws: Worksheet) -> None:
    ensure_spreadsheet_locale(spreadsheet)
    analysis_ws = get_or_create_analysis_worksheet(spreadsheet)

    current_team_a = (analysis_ws.acell("B2").value or "").strip()
    current_team_b = (analysis_ws.acell("B3").value or "").strip()

    matches_profile_rows = matches_ws.get("D2:H")

    team_candidates: list[str] = []
    for row in matches_profile_rows:
        for value in row[:2]:
            team_name = (value or "").strip()
            if team_name and team_name not in team_candidates:
                team_candidates.append(team_name)
            if len(team_candidates) >= 2:
                break
        if len(team_candidates) >= 2:
            break

    default_team_a = team_candidates[0] if len(team_candidates) >= 1 else ""
    default_team_b = team_candidates[1] if len(team_candidates) >= 2 else default_team_a

    team_a = current_team_a or default_team_a
    team_b = current_team_b or default_team_b
    if team_b == team_a:
        team_b = default_team_b if default_team_b != team_a else ""

    col_a_px, col_bc_px = estimate_analysis_column_widths(matches_profile_rows)

    write_analysis_layout(analysis_ws, team_a, team_b)
    style_dashboard(spreadsheet, analysis_ws, matches_ws, col_a_px, col_bc_px)
    apply_dashboard_conditional_formatting(spreadsheet, analysis_ws)
    protect_dashboard_sheet(spreadsheet, analysis_ws)


def deduplicate_matches_worksheet(worksheet: Worksheet) -> int:
    rows = worksheet.get_all_values()
    if not rows:
        return 0

    header = rows[0]
    data_rows = rows[1:]

    seen_ids: set[str] = set()
    deduped_rows: list[list[str]] = []
    duplicates_removed = 0

    for row in data_rows:
        normalized = row + [""] * (len(HEADERS) - len(row))
        normalized = normalized[: len(HEADERS)]
        match_id = normalized[0].strip()
        key = match_id or "|".join(normalized)

        if key in seen_ids:
            duplicates_removed += 1
            continue

        seen_ids.add(key)
        deduped_rows.append(normalized)

    if duplicates_removed == 0:
        return 0

    prepared_rows = [_prepare_row_for_user_entered(row) for row in deduped_rows]

    worksheet.clear()
    worksheet.update(
        range_name=f"A1:I{len(prepared_rows) + 1}",
        values=[header[: len(HEADERS)]] + prepared_rows,
        value_input_option=ValueInputOption.user_entered,
    )
    logging.info("Удалены дубликаты в листе '%s': %s", WORKSHEET_TITLE, duplicates_removed)
    return duplicates_removed


def _normalize_row_for_sheet(row: list[Any]) -> list[str]:
    normalized: list[str] = []
    for idx in range(len(HEADERS)):
        value = row[idx] if idx < len(row) else ""
        text_value = str(value).strip() if value is not None else ""
        if idx == 0 and text_value.startswith("'") and text_value[1:].isdigit():
            text_value = text_value[1:]
        normalized.append(text_value)
    return normalized


def _prepare_row_for_user_entered(row: list[str]) -> list[Any]:
    prepared: list[Any] = []
    for idx, raw_value in enumerate(row[: len(HEADERS)]):
        value = (raw_value or "").strip()
        if value.startswith("'"):
            value = value[1:]

        if idx in (0, 5, 6) and value.isdigit():
            prepared.append(int(value))
        else:
            prepared.append(value)

    while len(prepared) < len(HEADERS):
        prepared.append("")

    return prepared


def normalize_existing_matches_cells(worksheet: Worksheet) -> int:
    rows = worksheet.get_all_values()
    if len(rows) <= 1:
        return 0

    normalized_rows = [_normalize_row_for_sheet(row) for row in rows[1:]]
    prepared_rows = [_prepare_row_for_user_entered(row) for row in normalized_rows]

    worksheet.update(
        range_name=f"A2:I{len(prepared_rows) + 1}",
        values=prepared_rows,
        value_input_option=ValueInputOption.user_entered,
    )
    logging.info("Нормализованы форматы/префиксы в листе '%s': строк %s", WORKSHEET_TITLE, len(prepared_rows))
    return len(prepared_rows)


def apply_matches_column_formats(worksheet: Worksheet) -> None:
    worksheet.format("A2:A", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})
    worksheet.format("B2:B", {"numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}})
    worksheet.format("C2:C", {"numberFormat": {"type": "TIME", "pattern": "hh:mm"}})
    worksheet.format("F2:G", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})


def prepare_rows_for_upsert(
    worksheet: Worksheet,
    rows: list[list[Any]],
) -> tuple[list[list[Any]], list[tuple[int, list[str]]], int]:
    sheet_rows = worksheet.get_all_values()
    existing_rows = sheet_rows[1:] if len(sheet_rows) > 1 else []

    existing_by_id: dict[str, tuple[int, list[str]]] = {}
    for row_idx, row in enumerate(existing_rows, start=2):
        normalized = _normalize_row_for_sheet(row)
        match_id = normalized[0]
        if match_id and match_id not in existing_by_id:
            existing_by_id[match_id] = (row_idx, normalized)

    append_rows: list[list[Any]] = []
    updates_by_row: dict[int, list[str]] = {}
    skipped = 0
    batch_new_ids: set[str] = set()

    for row in rows:
        normalized_incoming = _normalize_row_for_sheet(row)
        match_id = normalized_incoming[0]
        if not match_id:
            skipped += 1
            continue

        if match_id in existing_by_id:
            row_number, existing = existing_by_id[match_id]
            if existing != normalized_incoming:
                updates_by_row[row_number] = normalized_incoming
                existing_by_id[match_id] = (row_number, normalized_incoming)
            else:
                skipped += 1
            continue

        if match_id in batch_new_ids:
            skipped += 1
            continue

        batch_new_ids.add(match_id)
        append_rows.append(row)

    updates = sorted(updates_by_row.items(), key=lambda item: item[0])
    return append_rows, updates, skipped


def _log_retry_failure(retry_state: RetryCallState) -> None:
    if retry_state.outcome is None or not retry_state.outcome.failed:
        return

    exc = retry_state.outcome.exception()
    print(f"Попытка {retry_state.attempt_number} провалилась: {exc}")


RETRYABLE_GOOGLE_API_EXCEPTIONS = (
    ConnectionError,
    urllib3.exceptions.ProtocolError,
    TransportError,
    ConnectionResetError,
    socket.timeout,
)


def google_api_retry():
    return retry(
        retry=retry_if_exception_type(RETRYABLE_GOOGLE_API_EXCEPTIONS),
        stop=stop_after_attempt(7),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        after=_log_retry_failure,
        reraise=True,
    )


@google_api_retry()
def append_rows_single_batch(rows: list[list[Any]]) -> None:
    creds_path = (GOOGLE_CREDS_PATH or "").strip()
    if not creds_path:
        raise RuntimeError("Переменная окружения GOOGLE_CREDS_PATH не задана")

    creds_file = Path(creds_path)

    if not creds_file.exists():
        raise RuntimeError(
            "Файл ключа сервисного аккаунта не найден. "
            f"Проверен путь: {creds_file}. "
            "Укажите корректный GOOGLE_CREDS_PATH."
        )

    client = build_gspread_client(str(creds_file))
    spreadsheet = open_or_create_spreadsheet(client)
    worksheet = get_matches_worksheet(spreadsheet)
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
    logging.info("Целевая таблица: %s", spreadsheet.title)
    logging.info("Ссылка на таблицу: %s", spreadsheet_url)

    # Один легкий запрос, чтобы понять, нужно ли добавлять заголовок.
    first_row = worksheet.row_values(1)
    is_sheet_empty = len(first_row) == 0

    incoming_rows = len(rows)
    filtered_rows, updates, skipped_rows = prepare_rows_for_upsert(worksheet, rows)
    logging.info(
        "Батч строк: всего=%s | к добавлению=%s | к обновлению=%s | пропущено=%s",
        incoming_rows,
        len(filtered_rows),
        len(updates),
        skipped_rows,
    )

    if not filtered_rows and not updates:
        logging.info("Новых или изменённых строк для применения нет")
        sync_analysis_dashboard(spreadsheet, worksheet)
        return

    payload = [HEADERS] + filtered_rows if is_sheet_empty else filtered_rows

    if filtered_rows:
        worksheet.append_rows(payload)  # type: ignore[call-arg]
        logging.info("Добавлено строк в лист '%s': %s", WORKSHEET_TITLE, len(filtered_rows))

    if updates:
        worksheet.batch_update(
            [
                {
                    "range": f"A{row_number}:I{row_number}",
                    "values": [_prepare_row_for_user_entered(row_values)],
                }
                for row_number, row_values in updates
            ],
            value_input_option=ValueInputOption.user_entered,
        )
        logging.info("Обновлено существующих строк в листе '%s': %s", WORKSHEET_TITLE, len(updates))

    deduplicate_matches_worksheet(worksheet)
    normalize_existing_matches_cells(worksheet)
    apply_matches_column_formats(worksheet)

    sync_analysis_dashboard(spreadsheet, worksheet)


@google_api_retry()
def sync_dashboard_only() -> None:
    creds_path = (GOOGLE_CREDS_PATH or "").strip()
    if not creds_path:
        raise RuntimeError("Переменная окружения GOOGLE_CREDS_PATH не задана")

    creds_file = Path(creds_path)

    if not creds_file.exists():
        raise RuntimeError(
            "Файл ключа сервисного аккаунта не найден. "
            f"Проверен путь: {creds_file}. "
            "Укажите корректный GOOGLE_CREDS_PATH."
        )

    client = build_gspread_client(str(creds_file))
    spreadsheet = open_or_create_spreadsheet(client)
    worksheet = get_matches_worksheet(spreadsheet)
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
    logging.info("Целевая таблица: %s", spreadsheet.title)
    logging.info("Ссылка на таблицу: %s", spreadsheet_url)
    logging.info("Батч строк: всего=0 | уникальных к добавлению=0 | пропущено дублей=0 (режим sync-only)")
    deduplicate_matches_worksheet(worksheet)
    normalize_existing_matches_cells(worksheet)
    apply_matches_column_formats(worksheet)
    sync_analysis_dashboard(spreadsheet, worksheet)


def run_ci_local_export(journal_handle) -> int:
    """Run export in CI without external Google API calls."""
    state = load_state()
    last_exported_date = state["last_exported_date"]
    append_workflow_journal("export_start", f"last_exported_date={last_exported_date}")

    new_files = discover_new_data_files(DATA_DIR, last_exported_date)
    last_processed_date: date | None = None

    if new_files:
        rows, last_processed_date = collect_rows_incrementally(new_files)
    else:
        latest_file = discover_latest_matches_file()
        rows = collect_rows_from_file(latest_file) if latest_file is not None else []
        last_processed_date = parse_iso_date(last_exported_date) or date.today()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ci_export_file = DATA_DIR / "ci_export_result.json"
    ci_export_file.write_text(
        json.dumps(
            {
                "mode": "ci_local_export",
                "rows": rows,
                "rows_count": len(rows),
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    if last_processed_date is not None:
        save_state(last_processed_date.isoformat())

    append_workflow_journal("export_done", f"mode=ci_local_export rows={len(rows)}")
    journal_handle.write(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
    )
    return 0


def main() -> int:
    journal_handle = start_terminal_journal("export_to_sheets")
    setup_logging()

    if CI_MODE:
        # CI mode writes export artifacts locally and skips any Google API interaction.
        try:
            return run_ci_local_export(journal_handle)
        except Exception as exc:
            logging.exception("Ошибка CI local export: %s", exc)
            append_workflow_journal("export_error", f"mode=ci_local_export error={exc}")
            journal_handle.write(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
            )
            return 1

    state = load_state()
    last_exported_date = state["last_exported_date"]
    logging.info("Текущий last_exported_date: %s", last_exported_date)
    append_workflow_journal("export_start", f"last_exported_date={last_exported_date}")

    new_files = discover_new_data_files(DATA_DIR, last_exported_date)
    if not new_files:
        latest_file = discover_latest_matches_file()
        if latest_file is not None:
            logging.info(
                "Новых файлов по state нет. Выполняю upsert из последнего файла: %s",
                latest_file.name,
            )
            rows_for_refresh = collect_rows_from_file(latest_file)
            try:
                append_rows_single_batch(rows_for_refresh)
            except Exception as exc:
                logging.exception("Ошибка refresh-upsert в Google Sheets: %s", exc)
                append_workflow_journal("export_error", f"mode=refresh_upsert error={exc}")
                journal_handle.write(
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
                )
                return 1
            append_workflow_journal("export_done", f"mode=refresh_upsert rows={len(rows_for_refresh)}")
            journal_handle.write(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
            )
            return 0

        logging.info("Новых файлов для экспорта нет. Выполняю синхронизацию analysis")
        try:
            sync_dashboard_only()
        except Exception as exc:
            logging.exception("Ошибка синхронизации analysis: %s", exc)
            append_workflow_journal("export_error", f"mode=sync_only error={exc}")
            journal_handle.write(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
            )
            return 1
        append_workflow_journal("export_done", "mode=sync_only")
        journal_handle.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
        )
        return 0

    rows, last_processed_date = collect_rows_incrementally(new_files)

    if last_processed_date is None:
        logging.warning("Нет корректно обработанных файлов, экспорт пропущен")
        append_workflow_journal("export_done", "mode=skipped_no_valid_files")
        journal_handle.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
        )
        return 0

    if not rows:
        logging.info("Новых матчей для отправки нет (файлы пустые или без валидных записей)")
        try:
            sync_dashboard_only()
        except Exception as exc:
            logging.exception("Ошибка синхронизации analysis: %s", exc)
            append_workflow_journal("export_error", f"mode=empty_rows_sync error={exc}")
            journal_handle.write(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
            )
            return 1
        save_state(last_processed_date.isoformat())
        append_workflow_journal("export_done", "mode=empty_rows_sync")
        journal_handle.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
        )
        return 0

    try:
        append_rows_single_batch(rows)
    except Exception as exc:
        logging.exception("Ошибка экспорта в Google Sheets: %s", exc)
        append_workflow_journal("export_error", f"mode=append error={exc}")
        journal_handle.write(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
        )
        return 1

    save_state(last_processed_date.isoformat())
    logging.info("Экспорт завершен: добавлено строк %s", len(rows))
    append_workflow_journal("export_done", f"mode=append rows={len(rows)}")
    journal_handle.write(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=export_to_sheets | action=run_close\n"
    )
    return 0


def export_to_sheets() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(main())
