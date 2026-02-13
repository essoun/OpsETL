from __future__ import annotations

from app.db import get_conn


def clear_stage_people() -> int:
    """
    Deletes all rows from dbo.stage_people.
    Returns rows deleted.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dbo.stage_people;")
        n = int(cur.fetchone()[0])
        cur.execute("DELETE FROM dbo.stage_people;")
        conn.commit()
        return n
    finally:
        conn.close()
