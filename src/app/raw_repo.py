from __future__ import annotations

from app.db import get_conn


def count_raw_orders() -> int:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dbo.raw_orders;")
        return int(cur.fetchone()[0])
    finally:
        conn.close()
