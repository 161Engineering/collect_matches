import logging
import os
from pathlib import Path

from collect_matches import collect_matches
from config import ensure_project_dirs
from export_to_sheets import export_to_sheets


CI_MODE = os.getenv("CI_MODE", "").strip().lower() == "true"


def main() -> int:
    ensure_project_dirs()

    if CI_MODE:
        # CI mode keeps pipeline outputs isolated from normal runtime folders.
        Path("ci_output").mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    try:
        collect_matches()
        return export_to_sheets()
    except Exception as exc:
        logging.exception("Pipeline failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
