from __future__ import annotations

from pathlib import Path

import pyodbc

from app.config import get_db_config

MIGRATIONS_DIR = Path(__file__).parent


def get_conn():
    cfg = get_db_config()

    if cfg.trusted:
        conn_str = (
            f"DRIVER={{{cfg.driver}}};"
            f"SERVER={cfg.server};"
            f"DATABASE={cfg.database};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
    else:
        raise RuntimeError("Non-trusted connection not implemented yet (set MSSQL_TRUSTED=true).")

    return pyodbc.connect(conn_str, autocommit=False)


def _split_go_batches(sql: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    for line in sql.splitlines():
        if line.strip().upper() == "GO":
            parts.append("\n".join(buf))
            buf = []
        else:
            buf.append(line)
    if buf:
        parts.append("\n".join(buf))
    return [p for p in parts if p.strip()]


def ensure_migrations_table(conn) -> None:
    sql = (MIGRATIONS_DIR / "001_create_schema_migrations.sql").read_text(encoding="utf-8")
    cur = conn.cursor()
    for batch in _split_go_batches(sql):
        cur.execute(batch)

def _version_from_filename(filename: str) -> int:
    # expects "002_something.sql"
    head = filename.split("_", 1)[0]
    return int(head)


def applied_migrations(conn) -> set[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM dbo.schema_migrations;")
    return {r[0] for r in cur.fetchall()}


def list_migration_files() -> list[Path]:
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


def apply_migrations() -> int:
    conn = get_conn()
    try:
        ensure_migrations_table(conn)
        conn.commit()

        done = applied_migrations(conn)
        files = list_migration_files()

        applied_count = 0
        for f in files:
            if f.name == "001_create_schema_migrations.sql":
                continue
            if f.name in done:
                continue

            sql = f.read_text(encoding="utf-8")
            cur = conn.cursor()

            for batch in _split_go_batches(sql):
                cur.execute(batch)

            version = _version_from_filename(f.name)

            cur.execute(
            "INSERT INTO dbo.schema_migrations(version, name, applied_at) VALUES (?, ?, SYSUTCDATETIME());",
             (version, f.name),
            )


            conn.commit()
            applied_count += 1
            print(f"applied ✅ {f.name}")

        return applied_count
    finally:
        conn.close()


