from __future__ import annotations

import pyodbc
from app.config import get_db_config


def get_conn():
    cfg = get_db_config()

    if not cfg.trusted:
        raise RuntimeError("Non-trusted connection not implemented yet (set MSSQL_TRUSTED=true).")

    conn_str = (
        f"DRIVER={{{cfg.driver}}};"
        f"SERVER={cfg.server};"
        f"DATABASE={cfg.database};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;"                  # ODBC 18 explicit
        "TrustServerCertificate=yes;"   # dev-friendly TLS
        "LoginTimeout=5;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


