# src/app/transform_schema.py
from __future__ import annotations

from typing import Any

from app.db import get_conn
from app.transform_framework import DatasetSpec, IndexSpec


def _sql_type(cast: str) -> str:
    c = cast.lower().strip()
    if c == "str":
        return "NVARCHAR(4000) NULL"
    if c == "int":
        return "INT NULL"
    if c == "float":
        return "FLOAT NULL"
    if c == "money":
        return "DECIMAL(19,4) NULL"
    if c == "date":
        return "DATE NULL"
    raise RuntimeError(f"Unknown cast kind for SQL type: {cast}")


def _split_schema_table(full: str) -> tuple[str, str]:
    s = (full or "").strip()
    if "." in s:
        a, b = s.split(".", 1)
        return a.strip(), b.strip()
    return "dbo", s


def ensure_final_table_from_spec(
    spec: DatasetSpec,
    *,
    drop_and_recreate: bool = False,
    confirm: str | None = None,
) -> None:
    """
    Creates spec.final_table (typed) based on spec.fields, and creates any indexes in spec.indexes.
    Safety: drop_and_recreate requires confirm == f"DROP_CREATE {spec.final_table}"
    """
    if drop_and_recreate:
        expected = f"DROP_CREATE {spec.final_table}"
        if confirm is None or confirm.strip() != expected:
            raise RuntimeError(
                "Safety check: drop_and_recreate requires --require-confirm.\n"
                f'Expected: --require-confirm "{expected}"'
            )

    schema, name = _split_schema_table(spec.final_table)
    full = f"{schema}.{name}"

    cols_sql = ",\n    ".join([f"[{fr.field}] {_sql_type(fr.cast)}" for fr in spec.fields])

    sql_drop = f"IF OBJECT_ID('{full}','U') IS NOT NULL DROP TABLE {full};"
    sql_create = f"""
    CREATE TABLE {full} (
        {cols_sql}
    );
    """

    conn = get_conn()
    try:
        cur = conn.cursor()

        if drop_and_recreate:
            cur.execute(sql_drop)
            conn.commit()

        # create if not exists
        cur.execute(
            """
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?;
            """,
            (schema, name),
        )
        exists = cur.fetchone() is not None

        if not exists:
            cur.execute(sql_create)
            conn.commit()

        # indexes
        for ix in (spec.indexes or []):
            _ensure_index(cur, full, ix)
        conn.commit()

        print(f"final_table ✅ ensured={spec.final_table}")
    finally:
        conn.close()


def _ensure_index(cur, full_table: str, ix: IndexSpec) -> None:
    """
    Creates an index if it does not exist (by name) when ix.if_not_exists=True.
    """
    idx_name = ix.name.strip()
    if not idx_name:
        raise RuntimeError("IndexSpec.name cannot be empty")

    if ix.if_not_exists:
        cur.execute(
            """
            SELECT 1
            FROM sys.indexes
            WHERE name = ? AND object_id = OBJECT_ID(?);
            """,
            (idx_name, full_table),
        )
        if cur.fetchone() is not None:
            return

    unique_sql = "UNIQUE " if ix.unique else ""
    cols_sql = ", ".join([f"[{c}]" for c in ix.columns])

    include_sql = ""
    if ix.include:
        include_cols = ", ".join([f"[{c}]" for c in ix.include])
        include_sql = f" INCLUDE ({include_cols})"

    where_sql = f" WHERE {ix.where}" if ix.where else ""

    sql = f"CREATE {unique_sql}INDEX [{idx_name}] ON {full_table} ({cols_sql}){include_sql}{where_sql};"
    cur.execute(sql)
