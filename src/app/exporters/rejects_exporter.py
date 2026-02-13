# src/app/exporters/rejects_exporter.py
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from app.db import get_conn

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_]+$")


def _safe_table_for_dataset(dataset: str) -> str:
    dataset = dataset.strip()
    if not _SAFE_NAME.match(dataset):
        raise ValueError("dataset must be alphanumeric/underscore only (e.g., people)")
    return f"dbo.{dataset}_rejects"


def _fetch_reject_rows(*, dataset: str, top: int = 0) -> tuple[list[str], list[tuple]]:
    """
    Returns (columns, rows) from dbo.<dataset>_rejects.
    Orders by rejected_at DESC if present; else created_at DESC if present.
    """
    table = _safe_table_for_dataset(dataset)
    top_sql = f"TOP ({int(top)}) " if top and top > 0 else ""

    with get_conn() as conn:
        cur = conn.cursor()

        # Verify table exists
        cur.execute(
            """
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name='dbo' AND t.name = ?;
            """,
            f"{dataset}_rejects",
        )
        if cur.fetchone() is None:
            raise RuntimeError(f"Rejects table not found: {table}")

        # Discover columns
        cur.execute(f"SELECT TOP (0) * FROM {table};")
        cols = [d[0] for d in cur.description]

        cols_l = {c.lower(): c for c in cols}
        if "rejected_at" in cols_l:
            order_col = cols_l["rejected_at"]
        elif "created_at" in cols_l:
            order_col = cols_l["created_at"]
        else:
            order_col = None

        order_sql = f" ORDER BY {order_col} DESC" if order_col else ""
        cur.execute(f"SELECT {top_sql} * FROM {table}{order_sql};")
        rows = [tuple(r) for r in cur.fetchall()]

    return cols, rows


def export_rejects_jsonl(*, dataset: str, out_path: str, top: int = 0) -> str:
    cols, rows = _fetch_reject_rows(dataset=dataset, top=top)

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            obj = {}
            for k, v in zip(cols, r):
                obj[k] = v.isoformat(sep=" ") if hasattr(v, "isoformat") else v
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    return str(out)


def export_rejects_csv(*, dataset: str, out_path: str, top: int = 0) -> str:
    cols, rows = _fetch_reject_rows(dataset=dataset, top=top)

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([v.isoformat(sep=" ") if hasattr(v, "isoformat") else v for v in r])

    return str(out)



