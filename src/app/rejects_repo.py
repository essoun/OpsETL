# src/app/rejects_repo.py
from __future__ import annotations

import json
from typing import Any

from app.db import get_conn


def count_rejects(dataset_name: str) -> int:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM dbo.dataset_rejects WHERE dataset_name = ?;",
            (dataset_name,),
        )
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def list_rejects(dataset_name: str, top: int = 20) -> list[dict[str, Any]]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP (?) row_num, reject_reasons, raw_json, source_file
            FROM dbo.dataset_rejects
            WHERE dataset_name = ?
            ORDER BY reject_id DESC;
            """,
            (top, dataset_name),
        )

        out: list[dict[str, Any]] = []
        for row_num, reasons, raw_json, source_file in cur.fetchall():
            out.append(
                {
                    "row_num": int(row_num),
                    "reasons": str(reasons),
                    "source_file": None if source_file is None else str(source_file),
                    "raw": json.loads(raw_json) if raw_json else {},
                }
            )
        return out
    finally:
        conn.close()
