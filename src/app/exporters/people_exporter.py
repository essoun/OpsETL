from __future__ import annotations

from pathlib import Path
import csv

from app.people_repo import list_people


def export_people_csv(out_path: str, top: int | None = None) -> Path:
    """
    Export dbo.people rows to a CSV file.
    If top is provided, exports only top N (most recent first).
    Returns the Path written.
    """
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    rows = list_people(top if top is not None else 10_000_000)

    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["person_id", "full_name", "created_at"])
        for pid, name, created in rows:
            w.writerow([pid, name, created])

    return p
