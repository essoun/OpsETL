from __future__ import annotations

from app.db import get_conn


def add_person(full_name: str) -> int:
    """
    Insert one person into dbo.people.
    Returns the inserted person_id.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO dbo.people(full_name) OUTPUT INSERTED.person_id VALUES (?);",
            (full_name,),
        )
        person_id = int(cur.fetchone()[0])
        conn.commit()
        return person_id
    finally:
        conn.close()


def list_people(top: int = 20) -> list[tuple[int, str, str]]:
    """
    Returns rows: (person_id, full_name, created_at_iso)
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP (?) person_id, full_name, created_at
            FROM dbo.people
            ORDER BY person_id DESC;
            """,
            (top,),
        )
        rows: list[tuple[int, str, str]] = []
        for person_id, full_name, created_at in cur.fetchall():
            rows.append((int(person_id), str(full_name), created_at.isoformat(sep=" ")))
        return rows
    finally:
        conn.close()


def find_people(like: str, top: int = 20) -> list[tuple[int, str, str]]:
    """
    Search by substring on full_name.
    Returns rows: (person_id, full_name, created_at_iso)
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        pattern = f"%{like}%"
        cur.execute(
            """
            SELECT TOP (?) person_id, full_name, created_at
            FROM dbo.people
            WHERE full_name LIKE ?
            ORDER BY person_id DESC;
            """,
            (top, pattern),
        )
        rows: list[tuple[int, str, str]] = []
        for person_id, full_name, created_at in cur.fetchall():
            rows.append((int(person_id), str(full_name), created_at.isoformat(sep=" ")))
        return rows
    finally:
        conn.close()


def delete_person(person_id: int) -> int:
    """
    Deletes a person by id.
    Returns number of rows deleted (0 or 1).
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.people WHERE person_id = ?;", (person_id,))
        deleted = int(cur.rowcount)
        conn.commit()
        return deleted
    finally:
        conn.close()

def get_person(person_id: int) -> tuple[int, str, str] | None:
    """
    Returns (person_id, full_name, created_at_iso) or None if not found.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT person_id, full_name, created_at
            FROM dbo.people
            WHERE person_id = ?;
            """,
            (person_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        pid, name, created_at = row
        return (int(pid), str(name), created_at.isoformat(sep=" "))
    finally:
        conn.close()

def update_person_name(person_id: int, full_name: str) -> int:
    """
    Updates full_name for a person_id.
    Returns rows updated (0 or 1).
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE dbo.people SET full_name = ? WHERE person_id = ?;",
            (full_name, person_id),
        )
        updated = int(cur.rowcount)
        conn.commit()
        return updated
    finally:
        conn.close()

def person_exists_by_name(full_name: str) -> bool:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM dbo.people WHERE full_name = ?;", (full_name,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def add_person_if_missing(full_name: str) -> tuple[bool, int | None]:
    """
    Returns (inserted?, person_id_if_inserted)
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT person_id FROM dbo.people WHERE full_name = ?;", (full_name,))
        row = cur.fetchone()
        if row is not None:
            return (False, None)

        cur.execute(
            "INSERT INTO dbo.people(full_name) OUTPUT INSERTED.person_id VALUES (?);",
            (full_name,),
        )
        person_id = int(cur.fetchone()[0])
        conn.commit()
        return (True, person_id)
    finally:
        conn.close()

      

