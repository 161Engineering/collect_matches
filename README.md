# Basketball Matches Parser (ipbl.pro)

Минималистичный проект для:
- сбора матчей с `ipbl.pro` в JSON
- экспорта новых матчей в Google Sheets
- обновления аналитического листа

## Структура

- `run.py` — последовательный запуск `collect_matches()` и `export_to_sheets()`
- `collect_matches.py` — сбор матчей и сохранение JSON в `data/`
- `export_to_sheets.py` — экспорт новых матчей в Google Sheets, состояние в `state/export_state.json`
- `config.py` — конфигурация проекта и переменные окружения

## Установка

1. Скопируйте `.env.example` в `.env` (или задайте переменные окружения другим способом).
2. Положите файл сервисного аккаунта Google, например `credentials.json` в корне проекта. Файл credentials.json является частью системы интеграции. Пожалуйста не распространяйте его третьим лицам.
3. Выполните:

Переменные из `.env` подгружаются автоматически при запуске скриптов.

```
install.bat
```

## Google Service Account

1. Создайте Service Account в Google Cloud.
2. Скачайте JSON-ключ и сохраните как `credentials.json` (или укажите свой путь в `GOOGLE_CREDS_PATH`).
3. Откройте Google Sheet и выдайте доступ e-mail сервисного аккаунта как минимум с ролью Editor.
4. Убедитесь, что имя таблицы совпадает с `SPREADSHEET_NAME`.

## Запуск

Ежедневный запуск:

```bat
run.bat
```

## Безопасность

- `credentials.json` исключен из Git
- файлы из `data/` и `state/` исключены из Git
- `data/.gitkeep` и `state/.gitkeep` сохраняют структуру папок в репозитории
