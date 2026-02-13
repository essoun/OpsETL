# src/app/typecast.py
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def to_str(v: Any) -> str:
    return str(v).strip()


def to_int(v: Any) -> int | None:
    s = str(v).strip()
    if s == "":
        return None
    try:
        return int(s)
    except ValueError:
        try:
            return int(float(s))
        except ValueError:
            return None


def to_float(v: Any) -> float | None:
    s = str(v).strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def to_decimal_money(v: Any) -> Decimal | None:
    s = str(v).strip()
    if s == "":
        return None
    s = s.replace("$", "").replace(",", "")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def to_date_any(v: Any) -> datetime | None:
    s = str(v).strip()
    if s == "":
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None
