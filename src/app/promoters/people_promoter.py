from __future__ import annotations

from typing import Any

from app.db import get_conn


def _as_int(v: Any) -> int | None:
    s = ("" if v is None else str(v)).strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _reject(cur, raw_pid: str, raw_name: str, raw_created: str, reason: str) -> None:
    cur.execute(
        """
        INSERT INTO dbo.people_rejects(raw_person_id, raw_full_name, raw_created_at, reason)
        VALUES (?, ?, ?, ?);
        """,
        (raw_pid, raw_name, raw_created, reason),
    )


def promote_people(from_table: str = "dbo.stage_people") -> tuple[int, int]:
    """
    Promote rows from staging (all NVARCHAR) into typed table (UPSERT).
    Returns (good, rejected).
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute(f"SELECT person_id, full_name, created_at FROM {from_table};")
        rows = cur.fetchall()

        good = 0
        bad = 0

        for person_id, full_name, created_at in rows:
            raw_pid = "" if person_id is None else str(person_id)
            raw_name = "" if full_name is None else str(full_name)
            raw_created = "" if created_at is None else str(created_at)

            pid = _as_int(raw_pid)
            name = raw_name.strip()

            if pid is None:
                bad += 1
                _reject(cur, raw_pid, raw_name, raw_created, "person_id not an integer")
                continue

            if not name:
                bad += 1
                _reject(cur, raw_pid, raw_name, raw_created, "full_name is empty")
                continue

            if len(name) > 200:
                bad += 1
                _reject(cur, raw_pid, raw_name, raw_created, "full_name too long (>200)")
                continue

            # created_at: parse in SQL; failures go to rejects
            try:
                cur.execute(
                    """
                    MERGE dbo.people_typed AS t
                    USING (
                        SELECT
                            ? AS person_id,
                            ? AS full_name,
                            CAST(? AS DATETIME2(0)) AS created_at
                    ) AS s
                    ON t.person_id = s.person_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            full_name = s.full_name,
                            created_at = s.created_at
                    WHEN NOT MATCHED THEN
                        INSERT (person_id, full_name, created_at)
                        VALUES (s.person_id, s.full_name, s.created_at);
                    """,
                    (pid, name, raw_created),
                )
                good += 1
            except Exception:
                bad += 1
                _reject(cur, raw_pid, raw_name, raw_created, "created_at not parseable to DATETIME2")

        conn.commit()
        return good, bad
    finally:
        conn.close()


