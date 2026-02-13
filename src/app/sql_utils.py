from __future__ import annotations

from app.db import get_conn


def count_table(table: str) -> int:
    """
    Counts rows in a table (expects a safe table name like dbo.stage_people).
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        return int(cur.fetchone()[0])
    finally:
        conn.close()
