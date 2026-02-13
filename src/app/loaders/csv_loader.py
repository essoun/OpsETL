# src/app/loaders/csv_loader.py
from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from app.db import get_conn


def normalize_col(name: str) -> str:
    s = (name or "").strip()
    s = s.replace(" ", "_")
    s = re.sub(r"[^A-Za-z0-9_]", "", s)
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"c_{s}"
    return s


def make_unique(cols: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    return out


def parse_table(full: str) -> tuple[str, str]:
    s = (full or "").strip()
    if "." in s:
        a, b = s.split(".", 1)
        return a.strip(), b.strip()
    return "dbo", s


def require_confirm(action: str, table: str, confirm: str | None) -> None:
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


def table_exists(cur, full_table: str) -> bool:
    schema, name = parse_table(full_table)
    cur.execute(
        """
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ?;
        """,
        (schema, name),
    )
    return cur.fetchone() is not None


def get_table_columns(cur, full_table: str) -> list[str]:
    schema, name = parse_table(full_table)
    cur.execute(
        """
        SELECT c.name
        FROM sys.columns c
        JOIN sys.tables t ON t.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ?
        ORDER BY c.column_id;
        """,
        (schema, name),
    )
    return [r[0] for r in cur.fetchall()]


def create_staging_table(cur, full_table: str, columns: list[str]) -> None:
    schema, name = parse_table(full_table)
    full = f"{schema}.{name}"
    cols_sql = ",\n    ".join([f"[{c}] NVARCHAR(4000) NULL" for c in columns])

    sql = f"""
    IF OBJECT_ID('{full}','U') IS NOT NULL
        DROP TABLE {full};

    CREATE TABLE {full} (
        {cols_sql}
    );
    """
    cur.execute(sql)


def truncate_table(cur, full_table: str) -> None:
    schema, name = parse_table(full_table)
    full = f"{schema}.{name}"
    cur.execute(f"TRUNCATE TABLE {full};")


def iter_csv_rows(
    csv_path: str,
    *,
    delimiter: str = ",",
    quotechar: str = '"',
    skiprows: int = 0,
) -> tuple[list[str], Iterable[list[str]]]:
    p = Path(csv_path)
    if not p.exists():
        raise RuntimeError(f"CSV not found: {csv_path}")

    f = p.open("r", encoding="utf-8", newline="")
    reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)

    # Skip leading rows before header
    for _ in range(skiprows):
        next(reader, None)

    raw_header = next(reader, None)
    if raw_header is None:
        f.close()
        raise RuntimeError("CSV appears empty (no header row).")

    norm_header = make_unique([normalize_col(h) for h in raw_header])

    def rows_iter():
        try:
            for row in reader:
                yield row
        finally:
            f.close()

    return norm_header, rows_iter()


def load_csv(
    *,
    csv_path: str,
    table: str,
    batch_size: int = 2000,
    drop_and_recreate: bool = True,
    truncate: bool = False,
    delimiter: str = ",",
    quotechar: str = '"',
    skiprows: int = 0,
    match_mode: str = "strict",  # "strict" | "set"
    confirm: str | None = None,
) -> None:
    """
    Loads a CSV into SQL Server staging table (all NVARCHAR).
    Safety:
      - drop_and_recreate requires confirm: "DROP_CREATE <table>"
      - truncate requires confirm: "TRUNCATE <table>" and table must exist
    Column matching:
      - strict: header order must equal table order (when not recreating)
      - set: same columns (any order) allowed (when not recreating)
    """
    header, rows = iter_csv_rows(csv_path, delimiter=delimiter, quotechar=quotechar, skiprows=skiprows)

    conn = get_conn()
    try:
        cur = conn.cursor()

        exists = table_exists(cur, table)

        if truncate:
            if not exists:
                raise RuntimeError(f"--truncate requires table to exist: {table}")
            require_confirm("TRUNCATE", table, confirm)
            truncate_table(cur, table)
            conn.commit()

        if drop_and_recreate:
            require_confirm("DROP_CREATE", table, confirm)
            create_staging_table(cur, table, header)
            conn.commit()
            exists = True

        if not exists:
            raise RuntimeError(f"Table does not exist: {table}. Use --drop-create or create it first.")

        # Verify column match if not recreating
        if not drop_and_recreate:
            tbl_cols = get_table_columns(cur, table)

            if match_mode == "strict":
                if [c.lower() for c in tbl_cols] != [c.lower() for c in header]:
                    raise RuntimeError(
                        "Column mismatch (strict). "
                        f"Table columns={tbl_cols} vs CSV columns={header}"
                    )
            elif match_mode == "set":
                if set(c.lower() for c in tbl_cols) != set(c.lower() for c in header):
                    raise RuntimeError(
                        "Column mismatch (set). "
                        f"Table columns={tbl_cols} vs CSV columns={header}"
                    )
            else:
                raise RuntimeError(f"Unknown match_mode: {match_mode}")

        placeholders = ",".join(["?"] * len(header))
        cols_sql = ",".join([f"[{c}]" for c in header])
        sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders});"

        batch: list[list[str]] = []
        total = 0

        for row in rows:
            # pad/trim row to header length
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            elif len(row) > len(header):
                row = row[: len(header)]

            batch.append(row)

            if len(batch) >= batch_size:
                cur.executemany(sql, batch)
                conn.commit()
                total += len(batch)
                print(f"loaded... {total}")
                batch = []

        if batch:
            cur.executemany(sql, batch)
            conn.commit()
            total += len(batch)

        print(f"load_csv ✅ table={table} rows={total} cols={len(header)}")
    finally:
        conn.close()