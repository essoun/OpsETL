#from __future__ import annotations

from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

def ensure_migrations_table(conn) -> None:
    sql = (MIGRATIONS_DIR / "001_create_schema_migrations.sql").read_text(encoding="utf-8")
    cur = conn.cursor()
    # Split on GO lines (simple but works for our scripts)
    parts = []
    buf = []
    for line in sql.splitlines():
        if line.strip().upper() == "GO":
            parts.append("\n".join(buf))
            buf = []
        else:
            buf.append(line)
    if buf:
        parts.append("\n".join(buf))

    for p in parts:
        if p.strip():
            cur.execute(p)

def applied_migrations(conn) -> set[str]:
    cur = conn.cursor()
    cur.execute("SELECT filename FROM dbo.schema_migrations;")
    return {r[0] for r in cur.fetchall()}

def list_migration_files() -> list[Path]:
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    return files

def apply_migrations() -> int:
    """
    Applies any pending migrations in filename order.
    Returns number of applied migrations.
    """
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

            # transaction per migration (simple version; we’ll harden later)
            parts = []
            buf = []
            for line in sql.splitlines():
                if line.strip().upper() == "GO":
                    parts.append("\n".join(buf))
                    buf = []
                else:
                    buf.append(line)
            if buf:
                parts.append("\n".join(buf))

            for p in parts:
                if p.strip():
                    cur.execute(p)

            cur.execute(
                "INSERT INTO dbo.schema_migrations(filename) VALUES (?);",
                (f.name,),
            )
            conn.commit()
            applied_count += 1
            print(f"applied ✅ {f.name}")

        return applied_count
    finally:
        conn.close()