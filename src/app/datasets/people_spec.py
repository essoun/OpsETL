# src/app/typecast.py
from __future__ import annotations

from datetime import datetime, date
from decimal import Decimal, InvalidOperation


def to_str(v: str) -> str:
    return str(v).strip()


def to_int(v: str) -> int | None:
    s = str(v).strip()
    if s == "":
        return None
    s = s.replace(",", "")
    try:
        return int(float(s))
    except ValueError:
        return None


def to_float(v: str) -> float | None:
    s = str(v).strip()
    if s == "":
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def to_decimal_money(v: str) -> Decimal | None:
    s = str(v).strip()
    if s == "":
        return None
    s = s.replace("$", "").replace(",", "")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def to_date_any(v) -> date | None:
    """
    Best-effort date parser for common formats.
    Returns a date (not datetime). Returns None if cannot parse.
    """
    if v is None:
        return None

    # If it's already a date/datetime
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v

    s = str(v).strip()
    if s == "":
        return None

    fmts = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",   # <-- added (matches your created_at style)
        "%m/%d/%Y",
        "%m/%d/%y",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%b %d %Y",
        "%B %d %Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f).date()
        except ValueError:
            pass

    # Try ISO datetime like 2016-01-01T00:00:00 or with Z suffix
    try:
        s2 = s[:-1] if s.endswith("Z") else s
        return datetime.fromisoformat(s2).date()
    except ValueError:
        return None

