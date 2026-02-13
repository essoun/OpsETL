from __future__ import annotations

from pathlib import Path
import csv

from app.people_repo import add_person_if_missing


def import_people_csv(in_path: str) -> dict[str, int]:
    """
    Imports people from a CSV with headers: person_id, full_name, created_at
    Inserts by full_name (skips duplicates by name).
    Returns counts: {"read": x, "inserted": y, "skipped": z}
    """
    p = Path(in_path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")

    read = 0
    inserted = 0
    skipped = 0

    with p.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if r.fieldnames is None or "full_name" not in r.fieldnames:
            raise ValueError("CSV must include a 'full_name' column")

        for row in r:
            read += 1
            name = (row.get("full_name") or "").strip()
            if not name:
                skipped += 1
                continue

            did_insert, _new_id = add_person_if_missing(name)
            if did_insert:
                inserted += 1
            else:
                skipped += 1

    return {"read": read, "inserted": inserted, "skipped": skipped}
