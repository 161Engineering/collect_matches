# Basketball Matches Parser

Проект собирает завершенные баскетбольные матчи из внешнего календаря, сохраняет их в JSON, делает upsert в Google Sheets и поддерживает аналитический лист `analysis`.

## Что делает pipeline

`run.py` выполняет шаги последовательно:

1. Проверяет доступ к Google Sheets и определяет дату последнего матча в таблице (`get_latest_match_date_from_sheet`).
2. Запускает `collect_matches` за период от даты из таблицы до сегодняшней даты.
3. Запускает `export_to_sheets`.
4. Отправляет Telegram-отчет (`SUCCESS`/`INFO`/`ERROR`, если заданы токен и chat id).
5. Удаляет локально сгенерированные JSON из `data/*.json` после успешного прохода.

Важно: если на этапе проверки таблицы нет доступа к Google Sheets, `run.py` завершает pipeline как `INFO` и возвращает код `0`.

## Основные файлы

- `run.py` - оркестрация всего пайплайна и Telegram-репорт.
- `collect_matches.py` - сбор матчей через Playwright и формирование parse-summary.
- `export_to_sheets.py` - upsert в лист `matches`, дедупликация, обновление листа `analysis`.
- `notifications/telegram_report.py` - форматирование и отправка уведомлений в Telegram.
- `config.py` - загрузка `.env` и общие пути/переменные окружения.
- `.github/workflows/run_scripts.yml` - production workflow для CI.

## Установка (локально)

1. Создайте и заполните `.env` на основе `.env.example`.
2. Положите JSON ключ сервисного аккаунта Google (например, `credentials.json`) в корень проекта.
3. Создайте виртуальное окружение и установите зависимости:

```bat
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

4. Установите браузер для Playwright:

```bat
playwright install chromium
```

## Переменные окружения

Переменные подхватываются из `.env` автоматически (`python-dotenv` в `config.py`).

Обязательные:

- `GOOGLE_CREDS_PATH` - путь к JSON ключу сервисного аккаунта (пример: `credentials.json`).
- `GOOGLE_SHEET_ID` - ID целевой Google таблицы.
- `SITE_CALENDAR_URL` - URL календаря источника (пример: `https://example.com/calendar/`).

Рекомендуемые:

- `SITE_NAME` - короткое имя источника для логов (если не задано, используется `источник`).
- `SPREADSHEET_NAME` - имя по умолчанию для отчетов/summary (fallback).

Опциональные (Telegram):

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `TELEGRAM_CHAT_ID_CUSTOMERS` (дополнительный получатель, например клиент)

Если Telegram-переменные не заданы, уведомления просто не отправляются.

## Запуск

Запуск:

```bat
.venv\Scripts\activate
python run.py
```

## Особенности `collect_matches.py`

- Работает в диапазоне дат `start_date..today`, где `start_date` приходит из `run.py` (дата последнего матча в таблице).
- Формат дат для внутренних параметров: `dd.mm.yyyy`.
- Собирает только завершенные матчи (`finished`/`заверш`).
- Автоматически определяет список лиг с календарной страницы.
- Использует batch-режим по вкладкам браузера (`batch_pages=10`).
- Делает повторный проход проблемных страниц и финальный retry для неразобранных.

Выходные данные парсинга:

- `data/matches_YYYY-MM-DD_YYYY-MM-DD_all-leagues*.json`
- `state/parse_summary.json`
- `data/workflow_journal.log`

## Особенности `export_to_sheets.py`

- Работает только с таблицей по `GOOGLE_SHEET_ID`.
- Лист матчей: `matches` (создается автоматически).
- Лист аналитики: `analysis` (создается/перестраивается автоматически).
- Выполняет upsert по `match_id`:
	- новые матчи добавляет,
	- измененные обновляет,
	- дубликаты удаляет.
- Нормализует типы/форматы колонок (`date`, `time`, numeric поля).

Режимы работы экспорта:

- `append` - есть новые файлы после `last_exported_date`.
- `refresh_upsert` - новых файлов нет, но найден последний `matches_*` файл.
- `sync_only` - данных для upsert нет, только синхронизация `analysis`.
- `empty_rows_sync` - новые файлы есть, но без валидных матчей.
- `skipped_no_valid_files` - не удалось обработать ни одного корректного файла.

Выходные данные экспорта:

- `state/export_state.json`
- `state/export_summary.json`
- `data/workflow_journal.log`

## CI (GitHub Actions)

Workflow: `.github/workflows/run_scripts.yml`

Триггеры:

- `pull_request` в `main`/`master`
- `schedule` (cron)
- `workflow_dispatch`

Secrets для CI:

- `GOOGLE_CREDENTIALS` - полный JSON сервисного аккаунта.
- `GOOGLE_SHEET_ID` - ID Google таблицы.
- `SITE_CALENDAR_URL` - URL календаря источника.
- `SITE_NAME` - имя источника для логов.
- `TELEGRAM_BOT_TOKEN` - токен Telegram-бота.
- `TELEGRAM_CHAT_ID` - ID чата/канала Telegram.
- `TELEGRAM_CHAT_ID_CUSTOMERS` - дополнительный ID получателя уведомлений.

В CI credentials записывается во временный `credentials.json` и удаляется в конце workflow.

## Безопасность

- `.env` и `credentials.json` исключены из Git.
- `data/`, `state/`, `logs/`, `user_data/` исключены из Git (кроме `.gitkeep`).
- URL/название источника не хардкодятся в коде, задаются через env/secrets.