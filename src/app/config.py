from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()  # loads .env from project root if present


@dataclass(frozen=True)
class DbConfig:
    driver: str
    server: str
    database: str
    trusted: bool


def _get_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def get_db_config() -> DbConfig:
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")
    server = os.getenv("MSSQL_SERVER", "")
    database = os.getenv("MSSQL_DATABASE", "")
    trusted = _get_bool("MSSQL_TRUSTED", True)

    if not server:
        raise RuntimeError("Missing MSSQL_SERVER in environment/.env")
    if not database:
        raise RuntimeError("Missing MSSQL_DATABASE in environment/.env")

    return DbConfig(driver=driver, server=server, database=database, trusted=trusted)

