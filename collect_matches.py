# финальный скрипт для парсинга матчей с сайта
import json
import random
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

from playwright.sync_api import TimeoutError, sync_playwright

from config import DATA_DIR, SITE_CALENDAR_URL, SITE_NAME, STATE_DIR


CALENDAR_URL = ""
SOURCE_SITE_NAME = (SITE_NAME or "").strip() or "источник"
TARGET_DATE = "01.03.2026"
OUTPUT_FILE = "matches_test_01032026.json"
JOURNAL_FILE = DATA_DIR / "workflow_journal.log"
PARSE_SUMMARY_FILE = STATE_DIR / "parse_summary.json"
SPEED_LEVEL = 6


def get_calendar_url() -> str:
    global CALENDAR_URL
    if CALENDAR_URL:
        return CALENDAR_URL

    raw_url = (SITE_CALENDAR_URL or "").strip()
    if not raw_url:
        raise RuntimeError(
            "Не задан SITE_CALENDAR_URL. Укажи URL календаря источника в secrets/.env."
        )

    CALENDAR_URL = raw_url if raw_url.endswith("/") else f"{raw_url}/"
    return CALENDAR_URL


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


def clamp_speed_level(value: int) -> int:
    return 10


def get_speed_profile(speed_level: int) -> dict[str, float | int]:
    return {
        "delay_min": 0.01,
        "delay_max": 0.02,
        "skip_human_prob": 1.0,
        "networkidle_timeout_ms": 1500,
        "batch_pages": 10,
        "long_pause_every": 20,
        "long_pause_prob": 0.0,
        "long_pause_min": 0.01,
        "long_pause_max": 0.02,
    }


SPEED_LEVEL = 10
SPEED_PROFILE = get_speed_profile(SPEED_LEVEL)


def set_speed_level(speed_level: int) -> None:
    global SPEED_LEVEL, SPEED_PROFILE
    SPEED_LEVEL = 10
    SPEED_PROFILE = get_speed_profile(SPEED_LEVEL)


def human_delay(min_s: float = 1.5, max_s: float = 4.0) -> None:
    min_effective = float(SPEED_PROFILE["delay_min"])
    max_effective = float(SPEED_PROFILE["delay_max"])
    min_s = max(0.01, min(min_s, min_effective))
    max_s = max(min_s, min(max_s, max_effective))
    time.sleep(random.uniform(min_s, max_s))


def simulate_human(page) -> None:
    if random.random() < float(SPEED_PROFILE["skip_human_prob"]):
        return

    width = page.viewport_size["width"]
    height = page.viewport_size["height"]
    page.mouse.move(
        random.randint(100, width - 100),
        random.randint(100, height - 100),
        steps=random.randint(5, 20),
    )
    page.mouse.wheel(0, random.randint(200, 600))
    human_delay()


def save_json(data: list[dict], path: str = OUTPUT_FILE) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def build_unique_output_path(file_name: str, output_dir: Path = DATA_DIR) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    source = Path(file_name)
    base_name = source.stem
    suffix = source.suffix or ".json"

    candidate = output_dir / f"{base_name}{suffix}"
    if not candidate.exists():
        return candidate

    version = 1
    while True:
        candidate = output_dir / f"{base_name}_v{version}{suffix}"
        if not candidate.exists():
            return candidate
        version += 1


def load_last_end_date(default_date: str = TARGET_DATE) -> str:
    def normalize_date(value: str) -> str | None:
        text = (value or "").strip()
        if not text:
            return None

        ddmmyyyy_date = parse_ddmmyyyy(text)
        if ddmmyyyy_date is not None:
            return text

        try:
            iso_date = date.fromisoformat(text)
            return to_ddmmyyyy(iso_date)
        except Exception:
            return None

    if JOURNAL_FILE.exists():
        try:
            lines = JOURNAL_FILE.read_text(encoding="utf-8").splitlines()
        except Exception:
            lines = []

        for line in reversed(lines):
            if "script=collect_matches" in line and "action=parse_done" in line:
                period_match = re.search(r"period=(\d{2}\.\d{2}\.\d{4})\.\.(\d{2}\.\d{2}\.\d{4})", line)
                if period_match:
                    normalized = normalize_date(period_match.group(2))
                    if normalized is not None:
                        return normalized

            if "action=state_update" in line:
                date_match = re.search(r"\blast_exported_date=(\d{4}-\d{2}-\d{2})\b", line)
                if date_match:
                    normalized = normalize_date(date_match.group(1))
                    if normalized is not None:
                        return normalized

    return default_date


def save_last_end_date(last_end_date: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with JOURNAL_FILE.open("a", encoding="utf-8") as log_file:
        log_file.write(
            f"{timestamp} | script=collect_matches | action=state_update | last_end_date={last_end_date}\n"
        )


def append_parse_log(
    run_started_at: datetime,
    run_finished_at: datetime,
    period_start: str,
    period_end: str,
    output_path: Path,
    matches_count: int,
) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    line = (
        f"{run_finished_at.strftime('%Y-%m-%d %H:%M:%S')} | "
        f"script=collect_matches | action=parse_done | "
        f"period={period_start}..{period_end} | matches={matches_count} | "
        f"file={output_path.name} | run_start={run_started_at.isoformat(timespec='seconds')} | "
        f"run_end={run_finished_at.isoformat(timespec='seconds')}\n"
    )
    with JOURNAL_FILE.open("a", encoding="utf-8") as log_file:
        log_file.write(line)


def build_output_file_name(
    start_date_str: str,
    end_date_str: str,
    selected_leagues: list[str] | None = None,
) -> str:
    start_date = parse_ddmmyyyy(start_date_str)
    end_date = parse_ddmmyyyy(end_date_str)
    if not start_date or not end_date:
        return OUTPUT_FILE

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    if selected_leagues is None:
        league_suffix = "all-leagues"
    else:
        count = len(selected_leagues)
        league_suffix = "1league" if count == 1 else f"{count}leagues"

    return f"matches_{start_date.isoformat()}_{end_date.isoformat()}_{league_suffix}.json"


def normalize_team_name(name: str) -> str:
    value = (name or "").strip().lower()
    value = re.sub(r"[^a-z0-9а-яё\s-]", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", "-", value)
    return re.sub(r"-+", "-", value).strip("-")


def normalize_division_name(name: str) -> str:
    return normalize_team_name(name)


def strip_division_tail(team_name: str, division_name: str) -> str:
    team = normalize_team_name(team_name)
    division = normalize_division_name(division_name)

    if not team or not division:
        return team

    suffix = f"-{division}"
    if team.endswith(suffix):
        return team[: -len(suffix)].rstrip("-")

    return team


def sanitize_division_name(division_raw: str, league_slug: str) -> str:
    raw = normalize_division_name(division_raw)
    league = normalize_division_name(league_slug)

    if not raw:
        return league or "unknown"

    if not league:
        return raw

    if raw == league:
        return raw

    if raw.startswith(f"{league}-"):
        tail = raw[len(league) + 1 :]
        if re.fullmatch(r"\d+(?:-\d+)?", tail):
            return league

    return raw


def to_iso_date(date_str: str) -> str:
    match = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", (date_str or "").strip())
    if not match:
        return date_str
    day, month, year = match.groups()
    return f"{year}-{month}-{day}"


def parse_ddmmyyyy(date_str: str) -> date | None:
    try:
        return datetime.strptime(date_str.strip(), "%d.%m.%Y").date()
    except Exception:
        return None


def to_ddmmyyyy(day_value: date) -> str:
    return day_value.strftime("%d.%m.%Y")


def _build_target_date_labels(target_date: str) -> list[str]:
    month_map = {
        "01": "января",
        "02": "февраля",
        "03": "марта",
        "04": "апреля",
        "05": "мая",
        "06": "июня",
        "07": "июля",
        "08": "августа",
        "09": "сентября",
        "10": "октября",
        "11": "ноября",
        "12": "декабря",
    }
    match = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", target_date)
    if not match:
        return []
    dd, mm, _ = match.groups()
    month = month_map.get(mm)
    if not month:
        return []

    day_int = int(dd)
    labels = [
        f"{day_int} {month}",
        f"{dd} {month}",
    ]
    return labels


def block_images(context) -> None:
    if hasattr(context, "_images_blocked"):
        return

    def route_handler(route, request):
        if request.resource_type == "image":
            route.abort()
            return
        route.continue_()

    context.route("**/*", route_handler)
    context._images_blocked = True


def discover_leagues_from_calendar(page) -> list[str]:
    leagues: set[str] = set()

    try:
        links = page.locator("a[href*='league=']")
        count = links.count()
        for i in range(count):
            href = links.nth(i).get_attribute("href") or ""
            if not href:
                continue
            absolute = urljoin(page.url, href)
            parsed = urlparse(absolute)
            params = parse_qs(parsed.query)
            league_values = params.get("league", [])
            for value in league_values:
                slug = value.strip().strip("/")
                if slug:
                    leagues.add(slug)
    except Exception:
        pass

    try:
        data_nodes = page.locator("[data-league]")
        data_count = data_nodes.count()
        for i in range(data_count):
            slug = (data_nodes.nth(i).get_attribute("data-league") or "").strip().strip("/")
            if slug:
                leagues.add(slug)
    except Exception:
        pass

    return sorted(leagues)


def build_calendar_url(league_slug: str, target_day: date) -> str:
    base_calendar_url = get_calendar_url()
    return (
        f"{base_calendar_url}?league={league_slug}"
        f"&Y={target_day.year}&M={target_day.month:02d}&D={target_day.day}"
    )


def chunked(items: list[str], size: int) -> list[list[str]]:
    if size <= 1:
        return [[item] for item in items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def parse_match(
    match_href: str,
    match_date_iso: str,
    match_time: str,
    team1_raw: str,
    team2_raw: str,
    score1_raw: str,
    score2_raw: str,
    division_hint: str,
    status_raw: str,
) -> dict | None:
    match_id_match = re.search(r"/calendar/(\d+)/?", match_href)
    if not match_id_match:
        return None

    match_id = int(match_id_match.group(1))

    status_text = (status_raw or "").lower()
    if "заверш" not in status_text and "finished" not in status_text:
        return None

    if not score1_raw.isdigit() or not score2_raw.isdigit():
        return None

    score1 = int(score1_raw)
    score2 = int(score2_raw)

    division = normalize_division_name(division_hint)
    if not division:
        division = "unknown"

    team1 = strip_division_tail(team1_raw, division)
    team2 = strip_division_tail(team2_raw, division)
    if not team1 or not team2:
        return None

    return {
        "match_id": match_id,
        "date": match_date_iso,
        "time": match_time,
        "team1": team1,
        "team2": team2,
        "score1": score1,
        "score2": score2,
        "division": division,
        "status": "finished",
    }


def extract_match_cards_from_day_block(day_block, page_url: str) -> list[dict]:
    cards = day_block.locator("a[href*='/calendar/']")
    return cards.evaluate_all(
        r"""
        (elements, baseUrl) => {
            const norm = (v) => (v || '').replace(/\s+/g, ' ').trim();
            return elements.map((el) => {
                const href = el.getAttribute('href') || '';
                const abs = href ? new URL(href, baseUrl).toString() : '';

                const teamNodes = el.querySelectorAll('.teams .team .name');
                const scoreNodes = el.querySelectorAll('.score .val');

                return {
                    match_href: abs,
                    match_time: norm(el.querySelector('.time')?.textContent || ''),
                    team1_raw: norm(teamNodes[0]?.textContent || ''),
                    team2_raw: norm(teamNodes[1]?.textContent || ''),
                    score1_raw: norm(scoreNodes[0]?.textContent || ''),
                    score2_raw: norm(scoreNodes[1]?.textContent || ''),
                    division_raw: norm(el.querySelector('.league')?.textContent || ''),
                    status_raw: norm(el.querySelector('.status')?.textContent || ''),
                };
            });
        }
        """,
        page_url,
    )


def append_match_cards(
    match_cards: list[dict],
    league_slug: str,
    target_date_iso: str,
    matches_by_id: dict[str, dict],
    leagues_with_matches: set[str],
) -> int:
    added_count = 0

    for card in match_cards:
        try:
            match_href = card.get("match_href", "")
            if not match_href or not re.search(r"/calendar/\d+/?", match_href):
                continue

            match_time = str(card.get("match_time", "")).strip()
            team1_raw = str(card.get("team1_raw", "")).strip()
            team2_raw = str(card.get("team2_raw", "")).strip()
            score1_raw = str(card.get("score1_raw", "")).strip()
            score2_raw = str(card.get("score2_raw", "")).strip()
            division_raw = str(card.get("division_raw", "")).strip()
            division_hint = sanitize_division_name(division_raw, league_slug)
            status_raw = str(card.get("status_raw", "")).strip()

            parsed_match = parse_match(
                match_href=match_href,
                match_date_iso=target_date_iso,
                match_time=match_time,
                team1_raw=team1_raw,
                team2_raw=team2_raw,
                score1_raw=score1_raw,
                score2_raw=score2_raw,
                division_hint=division_hint,
                status_raw=status_raw,
            )
            if not parsed_match:
                continue

            match_id = parsed_match["match_id"]
            is_new = match_id not in matches_by_id
            matches_by_id[match_id] = parsed_match
            if is_new:
                added_count += 1
                leagues_with_matches.add(league_slug)
        except Exception:
            continue

    return added_count


def process_page_for_league(
    current_page,
    league_slug: str,
    target_date: str,
    target_date_iso: str,
    target_labels: list[str],
    matches_by_id: dict[str, dict],
    leagues_with_matches: set[str],
) -> tuple[str, int]:
    try:
        simulate_human(current_page)

        search_timeout_ms = max(2500, int(SPEED_PROFILE["networkidle_timeout_ms"]))
        target_day_block, day_block_source = find_target_day_block(current_page, target_labels, search_timeout_ms)

        if target_day_block is None:
            print(f"[INFO] NO_MATCHES {target_date} {league_slug}: матчей на странице не найдено")
            return "no_matches", 0

        if day_block_source == "fallback":
            print(f"[WARN] Использован fallback day-блок для {league_slug} ({target_date})")

        match_cards = extract_match_cards_from_day_block(target_day_block, current_page.url)
        if len(match_cards) == 0:
            print(f"[INFO] NO_MATCHES {target_date} {league_slug}: карточек=0")
            return "no_matches", 0

        added = append_match_cards(match_cards, league_slug, target_date_iso, matches_by_id, leagues_with_matches)
        print(f"[INFO] {league_slug} {target_date}: карточек={len(match_cards)}, добавлено={added}")
        return "ok", added
    except Exception as exc:
        print(f"[WARN] PARSE_FAIL {target_date} {league_slug}: {exc}")
        return "parse_fail", 0


def wait_until_page_ready(page) -> None:
    fast_mode = SPEED_LEVEL >= 10
    dom_timeout_ms = 7000 if fast_mode else 30000
    load_timeout_ms = 7000 if fast_mode else 30000
    networkidle_timeout_ms = int(SPEED_PROFILE["networkidle_timeout_ms"])
    ready_state_timeout_ms = 2000 if fast_mode else 10000
    selector_timeout_ms = 2500 if fast_mode else 15000

    # 1) Базовая готовность DOM
    page.wait_for_load_state("domcontentloaded", timeout=dom_timeout_ms)

    # 2) Полная загрузка документа
    page.wait_for_load_state("load", timeout=load_timeout_ms)

    # 3) Стабилизация сетевой активности
    try:
        page.wait_for_load_state("networkidle", timeout=networkidle_timeout_ms)
    except TimeoutError:
        # На некоторых страницах фоновые запросы постоянные — продолжаем после load.
        pass

    # 4) Дополнительная проверка готовности браузера
    try:
        page.wait_for_function("document.readyState === 'complete'", timeout=ready_state_timeout_ms)
    except Exception:
        pass

    # 5) Ждём появления контейнеров с матчами/днями
    try:
        page.wait_for_selector(".day, a.matche", timeout=selector_timeout_ms)
    except Exception:
        pass


def find_target_day_block(page, target_labels: list[str], search_timeout_ms: int) -> tuple[Any | None, str]:
    fast_mode = SPEED_LEVEL >= 10
    retries = 1 if fast_mode else 2
    fallback_candidate = None

    for attempt in range(1, retries + 1):
        try:
            page.wait_for_selector(".day, a.matche", timeout=search_timeout_ms)
        except Exception:
            pass

        day_blocks = page.locator(".day")
        day_blocks_count = day_blocks.count()
        if day_blocks_count == 0:
            time.sleep(0.02 if fast_mode else 0.12)
            continue

        for d_idx in range(day_blocks_count):
            block = day_blocks.nth(d_idx)
            date_text = ""
            try:
                date_text = re.sub(r"\s+", " ", (block.locator(".date").first.inner_text() or "").strip().lower())
            except Exception:
                date_text = re.sub(r"\s+", " ", (block.inner_text() or "").strip().lower())

            if any(label in date_text for label in target_labels):
                return block, "target"

            try:
                has_match_cards = block.locator("a[href*='/calendar/']").count() > 0
            except Exception:
                has_match_cards = False

            if has_match_cards and fallback_candidate is None:
                fallback_candidate = block

        if attempt < retries:
            page.mouse.wheel(0, 250)
            time.sleep(0.02 if fast_mode else 0.15)

    if fallback_candidate is not None:
        return fallback_candidate, "fallback"

    return None, "none"


def fetch_calendar(start_date_str: str, end_date_str: str, selected_leagues: list[str] | None = None) -> tuple[list[dict], dict[str, int | list[str]]]:
    matches_by_id: dict[str, dict] = {}
    leagues_with_matches: set[str] = set()
    all_site_leagues: list[str] = []
    unresolved_final_count = 0
    final_page_status: dict[tuple[str, str], str] = {}

    start_date = parse_ddmmyyyy(start_date_str)
    end_date = parse_ddmmyyyy(end_date_str)
    if not start_date or not end_date:
        print("[ERROR] Некорректный формат даты. Используй dd.mm.yyyy")
        return [], {
            "total_leagues_on_site": 0,
            "leagues_with_matches": 0,
            "leagues_without_matches": 0,
            "unparsed_pages": 0,
        }

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    leagues: list[str] = []

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir="user_data",
            headless=False,
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            ignore_https_errors=True,
        )

        if context.pages:
            page = context.pages[0]
        else:
            page = context.new_page()

        block_images(context)
        page.on("dialog", lambda d: d.accept())

        if selected_leagues:
            leagues = sorted(set([item.strip().strip("/") for item in selected_leagues if item.strip()]))
        else:
            try:
                page.goto(get_calendar_url(), wait_until="domcontentloaded", timeout=45000)
                try:
                    page.wait_for_load_state("networkidle", timeout=int(SPEED_PROFILE["networkidle_timeout_ms"]))
                except TimeoutError:
                    pass
                simulate_human(page)
            except Exception as exc:
                print(f"[ERROR] Не удалось открыть календарь для определения лиг ({SOURCE_SITE_NAME}): {exc}")
                context.close()
                return [], {
                    "total_leagues_on_site": 0,
                    "leagues_with_matches": 0,
                    "leagues_without_matches": 0,
                    "unparsed_pages": 0,
                }

            all_site_leagues = discover_leagues_from_calendar(page)
            leagues = discover_leagues_from_calendar(page)

        if not all_site_leagues:
            all_site_leagues = leagues.copy()

        if not leagues:
            print("[ERROR] Не удалось определить список лиг. Укажи их вручную через ввод.")
            context.close()
            return [], {
                "total_leagues_on_site": len(all_site_leagues),
                "leagues_with_matches": 0,
                "leagues_without_matches": len(all_site_leagues),
                "unparsed_pages": 0,
            }

        print(f"[INFO] Лиг в обработке: {len(leagues)}")

        batch_pages = int(SPEED_PROFILE["batch_pages"])
        page_pool = [page]
        while len(page_pool) < batch_pages:
            extra = context.new_page()
            extra.on("dialog", lambda d: d.accept())
            page_pool.append(extra)

        if batch_pages > 1:
            print(f"[INFO] Включен batch-режим: {batch_pages} страниц одновременно")

        total_days = (end_date - start_date).days + 1
        total_steps = total_days * len(leagues)
        step = 0

        deferred_tasks: list[tuple[date, str]] = []
        deferred_seen: set[tuple[str, str]] = set()

        current_day = start_date
        while current_day <= end_date:
            target_date = to_ddmmyyyy(current_day)
            target_date_iso = current_day.isoformat()
            target_labels = _build_target_date_labels(target_date)

            for league_chunk in chunked(leagues, batch_pages):
                prepared_batch: list[tuple[Any, str, str]] = []

                for page_index, league_slug in enumerate(league_chunk):
                    current_page = page_pool[page_index]

                    step += 1
                    league_calendar_url = build_calendar_url(league_slug, current_day)
                    print(f"[INFO] ({step}/{total_steps}) {league_calendar_url}")

                    try:
                        current_page.goto(league_calendar_url, wait_until="commit", timeout=45000)
                        prepared_batch.append((current_page, league_slug, league_calendar_url))
                    except Exception as exc:
                        print(f"[WARN] Не удалось открыть {league_calendar_url}: {exc}")
                        continue

                for current_page, league_slug, league_calendar_url in prepared_batch:
                    page_key = (current_day.isoformat(), league_slug)
                    page_ready = True
                    try:
                        wait_until_page_ready(current_page)
                    except Exception as exc:
                        print(f"[WARN] Страница загружена не полностью {league_calendar_url}: {exc}")
                        page_ready = False

                    status, _ = process_page_for_league(
                        current_page,
                        league_slug,
                        target_date,
                        target_date_iso,
                        target_labels,
                        matches_by_id,
                        leagues_with_matches,
                    )

                    if page_ready and status in ("ok", "no_matches"):
                        final_page_status[page_key] = status
                    else:
                        print(f"[RETRY] Повторный проход: {league_calendar_url}")
                        retry_status = "parse_fail"
                        try:
                            current_page.goto(league_calendar_url, wait_until="commit", timeout=45000)
                            wait_until_page_ready(current_page)
                            retry_status, _ = process_page_for_league(
                                current_page,
                                league_slug,
                                target_date,
                                target_date_iso,
                                target_labels,
                                matches_by_id,
                                leagues_with_matches,
                            )
                        except Exception as exc:
                            print(f"[RETRY] Ошибка повторного прохода {league_calendar_url}: {exc}")

                        if retry_status in ("ok", "no_matches"):
                            print(f"[RETRY] Успешно: {league_calendar_url}")
                            final_page_status[page_key] = retry_status
                        else:
                            key = (current_day.isoformat(), league_slug)
                            if key not in deferred_seen:
                                deferred_seen.add(key)
                                deferred_tasks.append((current_day, league_slug))
                            print(f"[RETRY] Неуспешно, отложено на финальный проход: {league_calendar_url}")

                    pause_every = int(SPEED_PROFILE["long_pause_every"])
                    if pause_every > 0 and step % pause_every == 0 and random.random() < float(SPEED_PROFILE["long_pause_prob"]):
                        human_delay(float(SPEED_PROFILE["long_pause_min"]), float(SPEED_PROFILE["long_pause_max"]))

            current_day += timedelta(days=1)

        if deferred_tasks:
            print(f"[RETRY-END] Запускаю финальный проход для {len(deferred_tasks)} страниц")
            closed_tabs = 0
            for pooled_page in page_pool:
                try:
                    if not pooled_page.is_closed():
                        pooled_page.close()
                        closed_tabs += 1
                except Exception:
                    pass

            print(f"[RETRY-END] Закрыто вкладок перед финальным проходом: {closed_tabs}")

            final_page = context.new_page()
            final_page.on("dialog", lambda d: d.accept())
            unresolved: list[tuple[date, str]] = []

            for day_value, league_slug in deferred_tasks:
                target_date = to_ddmmyyyy(day_value)
                target_date_iso = day_value.isoformat()
                target_labels = _build_target_date_labels(target_date)
                league_calendar_url = build_calendar_url(league_slug, day_value)

                print(f"[RETRY-END] {league_calendar_url}")
                final_status = "parse_fail"
                try:
                    final_page.goto(league_calendar_url, wait_until="commit", timeout=45000)
                    wait_until_page_ready(final_page)
                    final_status, _ = process_page_for_league(
                        final_page,
                        league_slug,
                        target_date,
                        target_date_iso,
                        target_labels,
                        matches_by_id,
                        leagues_with_matches,
                    )
                except Exception as exc:
                    print(f"[RETRY-END] Ошибка: {league_calendar_url} -> {exc}")

                if final_status not in ("ok", "no_matches"):
                    unresolved.append((day_value, league_slug))
                    final_page_status[(day_value.isoformat(), league_slug)] = "parse_fail"
                else:
                    final_page_status[(day_value.isoformat(), league_slug)] = final_status

            if unresolved:
                unresolved_final_count = len(unresolved)
                print(f"[RETRY-END] Не удалось обработать {len(unresolved)} страниц после финального прохода")
                for day_value, league_slug in unresolved:
                    print(f"[RETRY-END] FAIL {day_value.isoformat()} {league_slug}")
            else:
                print("[RETRY-END] Финальный проход завершен успешно")

        context.close()

    matches = sorted(matches_by_id.values(), key=lambda item: int(item["match_id"]))

    total_leagues_on_site = len(set(all_site_leagues))
    leagues_with_matches_count = len(leagues_with_matches)
    leagues_without_matches_list = sorted([league for league in set(all_site_leagues) if league not in leagues_with_matches])
    leagues_without_matches_count = len(leagues_without_matches_list)
    no_match_pages_count = sum(1 for status in final_page_status.values() if status == "no_matches")
    total_pages = total_days * len(leagues)
    parsed_pages = max(0, total_pages - unresolved_final_count)
    stats = {
        "total_leagues_on_site": total_leagues_on_site,
        "leagues_with_matches": leagues_with_matches_count,
        "leagues_without_matches": leagues_without_matches_count,
        "leagues_without_matches_list": leagues_without_matches_list,
        "no_match_pages": no_match_pages_count,
        "unparsed_pages": unresolved_final_count,
        "total_pages": total_pages,
        "parsed_pages": parsed_pages,
    }
    return matches, stats


def save_parse_summary(start_date_str: str, end_date_str: str, matches_count: int, stats: dict[str, int | list[str]]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "period_start": start_date_str,
        "period_end": end_date_str,
        "matches_count": matches_count,
        "leagues_with_matches": int(stats.get("leagues_with_matches", 0)),
        "leagues_without_matches": int(stats.get("leagues_without_matches", 0)),
        "total_pages": int(stats.get("total_pages", 0)),
        "parsed_pages": int(stats.get("parsed_pages", 0)),
        "no_match_pages": int(stats.get("no_match_pages", 0)),
        "unparsed_pages": int(stats.get("unparsed_pages", 0)),
    }
    PARSE_SUMMARY_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main(start_date_override: str | None = None) -> None:
    journal_handle = start_terminal_journal("collect_matches")
    run_started_at = datetime.now()
    today_str = to_ddmmyyyy(date.today())
    default_start_date = (start_date_override or "").strip() or load_last_end_date(TARGET_DATE)

    start_date_str = default_start_date
    end_date_str = today_str
    print(f"[INFO] Период парсинга: {start_date_str} .. {end_date_str}")

    start_date = parse_ddmmyyyy(start_date_str)
    end_date = parse_ddmmyyyy(end_date_str)
    if start_date and end_date and start_date > end_date:
        start_date, end_date = end_date, start_date
        start_date_str = to_ddmmyyyy(start_date)
        end_date_str = to_ddmmyyyy(end_date)

    print(f"[INFO] Скорость зафиксирована: {SPEED_LEVEL}")

    selected_leagues: list[str] | None = None
    print("[INFO] Режим лиг: автоматически собрать все доступные лиги с сайта")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with JOURNAL_FILE.open("a", encoding="utf-8") as log_file:
        log_file.write(
            f"{run_started_at.strftime('%Y-%m-%d %H:%M:%S')} | "
            f"script=collect_matches | action=parse_start | period={start_date_str}..{end_date_str}\n"
        )

    matches, stats = fetch_calendar(start_date_str=start_date_str, end_date_str=end_date_str, selected_leagues=selected_leagues)
    output_file_name = build_output_file_name(
        start_date_str,
        end_date_str,
        selected_leagues=selected_leagues,
    )
    output_path = build_unique_output_path(output_file_name)
    save_json(matches, str(output_path))

    run_finished_at = datetime.now()
    append_parse_log(
        run_started_at=run_started_at,
        run_finished_at=run_finished_at,
        period_start=start_date_str,
        period_end=end_date_str,
        output_path=output_path,
        matches_count=len(matches),
    )
    save_last_end_date(end_date_str)
    save_parse_summary(start_date_str, end_date_str, len(matches), stats)

    print(f"Собрано матчей: {len(matches)}")
    print(f"Всего лиг на сайте: {stats['total_leagues_on_site']}")
    print(f"Лиг с матчами за период: {stats['leagues_with_matches']}")
    print(f"Лиг без матчей за период: {stats['leagues_without_matches']}")
    leagues_without_matches_list = stats.get("leagues_without_matches_list", [])
    if isinstance(leagues_without_matches_list, list) and leagues_without_matches_list:
        print("Лиги без матчей за период:")
        for league_slug in leagues_without_matches_list:
            print(f"- {league_slug}")
    print(f"Страниц без матчей (NO_MATCHES): {stats['no_match_pages']}")
    print(f"Страниц не удалось отпарсить: {stats['unparsed_pages']}")
    print(f"Страниц обработано успешно: {stats['parsed_pages']} из {stats['total_pages']}")
    print(f"Сохранено в файл: {output_path}")
    print(f"Лог обновлен: {JOURNAL_FILE}")

    journal_handle.write(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | script=collect_matches | action=run_close\n"
    )


def collect_matches(start_date_override: str | None = None) -> None:
    main(start_date_override=start_date_override)


if __name__ == "__main__":
    main()
