from __future__ import annotations

from app.db import get_conn


def _require_confirm(action: str, table: str, confirm: str | None) -> None:
    expected = f"{action} {table}"
    if confirm is None:
        raise RuntimeError(
            "Safety check: this action requires --require-confirm.\n"
            f'Expected: --require-confirm "{expected}"'
        )
    if confirm.strip() != expected:
        raise RuntimeError(
            "Safety check: confirmation did not match.\n"
            f"Expected: {expected}\n"
            f"Got:      {confirm.strip()}"
        )


def truncate_table(*, table: str, confirm: str | None) -> int:
    """
    TRUNCATE the given table (fast delete all rows).
    Returns rows_before (count before truncate).
    Requires: --require-confirm "TRUNCATE <table>"
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        _require_confirm("TRUNCATE", table, confirm)

        cur.execute(f"SELECT COUNT(*) FROM {table};")
        before = int(cur.fetchone()[0])

        cur.execute(f"TRUNCATE TABLE {table};")
        conn.commit()
        return before
    finally:
        conn.close()
