# src/app/transform_framework.py
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Callable, Any

from app.db import get_conn
from app.typecast import to_int, to_float, to_decimal_money, to_date_any, to_str


# ---------- Rule specs ----------
@dataclass(frozen=True)
class FieldRule:
    field: str
    source: str
    cast: str  # "str"|"int"|"float"|"money"|"date"
    required: bool = False


@dataclass(frozen=True)
class RangeRule:
    field: str
    min: float | int | None = None
    max: float | int | None = None


@dataclass(frozen=True)
class AllowedRule:
    field: str
    allowed: set[str]


@dataclass(frozen=True)
class CrossRule:
    name: str
    fn: Callable[[dict[str, Any]], bool]  # True => ok


@dataclass(frozen=True)
class IndexSpec:
    name: str
    columns: list[str]
    unique: bool = False
    include: list[str] | None = None
    where: str | None = None
    if_not_exists: bool = True


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    stg_table: str
    final_table: str
    fields: list[FieldRule]
    required: list[str] | None = None
    ranges: list[RangeRule] | None = None
    allowed: list[AllowedRule] | None = None
    cross: list[CrossRule] | None = None
    indexes: list[IndexSpec] | None = None


# ---------- Casting ----------
def cast_value(kind: str, v: Any):
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "nan":
        return None

    kind = kind.lower()
    if kind == "str":
        return to_str(s)
    if kind == "int":
        return to_int(s)
    if kind == "float":
        return to_float(s)
    if kind == "money":
        return to_decimal_money(s)
    if kind == "date":
        return to_date_any(s)

    raise RuntimeError(f"Unknown cast kind: {kind}")


def row_hash(raw: dict[str, Any]) -> bytes:
    payload = json.dumps(raw, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(payload).digest()


# ---------- Transform runner ----------
def transform_dataset(
    spec: DatasetSpec,
    *,
    source_file: str | None = None,
    truncate_final: bool = False,
    truncate_rejects: bool = False,
    batch_size: int = 1000,
) -> None:
    """
    Reads staging rows, validates, writes to final + dataset_rejects.

    Behavior:
      - If truncate_final=True, final table is truncated and we re-insert everything.
      - If truncate_final=False, we INSERT-IF-MISSING by primary key (assumes first field is PK).
        This prevents duplicate key crashes on reruns.

    Assumes:
      - spec.final_table exists and matches spec.fields order/types.
      - dbo.dataset_rejects exists.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        if truncate_final:
            cur.execute(f"TRUNCATE TABLE {spec.final_table};")
            conn.commit()

        if truncate_rejects:
            cur.execute("DELETE FROM dbo.dataset_rejects WHERE dataset_name = ?;", (spec.name,))
            conn.commit()

        # Pull staging columns (as defined in FieldRule.source)
        stg_cols = [fr.source for fr in spec.fields]
        stg_select_cols = ", ".join([f"[{c}]" for c in stg_cols])
        cur.execute(f"SELECT {stg_select_cols} FROM {spec.stg_table};")

        good_rows: list[list[Any]] = []
        reject_rows: list[tuple] = []
        total = 0
        good = 0
        bad = 0

        # Precompute validators
        required_set = set((spec.required or []))
        range_rules = spec.ranges or []
        allowed_rules = spec.allowed or []
        cross_rules = spec.cross or []

        # Insert SQL
        final_cols = [fr.field for fr in spec.fields]
        final_cols_sql = ", ".join([f"[{c}]" for c in final_cols])
        placeholders = ", ".join(["?"] * len(final_cols))

        # Assume first field is PK for idempotent insert-if-missing
        pk_col = final_cols[0]

        # If we're not truncating, do insert-if-missing to avoid PK duplicates on reruns
        if truncate_final:
            insert_final_sql = f"INSERT INTO {spec.final_table} ({final_cols_sql}) VALUES ({placeholders});"
            needs_pk_dup_param = False
        else:
            insert_final_sql = f"""
            INSERT INTO {spec.final_table} ({final_cols_sql})
            SELECT {placeholders}
            WHERE NOT EXISTS (
              SELECT 1 FROM {spec.final_table} WHERE [{pk_col}] = ?
            );
            """
            needs_pk_dup_param = True

        insert_reject_sql = """
        INSERT INTO dbo.dataset_rejects(dataset_name, source_file, row_num, row_hash, reject_reasons, raw_json)
        VALUES (?,?,?,?,?,?);
        """

        rownum = 0
        for r in cur.fetchall():
            rownum += 1
            total += 1

            raw = {stg_cols[i]: r[i] for i in range(len(stg_cols))}
            typed: dict[str, Any] = {}
            reasons: list[str] = []

            # Cast each field + FieldRule.required
            for fr in spec.fields:
                v = raw.get(fr.source)
                tv = cast_value(fr.cast, v)
                typed[fr.field] = tv

                if fr.required and tv is None:
                    reasons.append(f"required:{fr.field}")

            # Required list (additional)
            for f in required_set:
                if typed.get(f) is None:
                    reasons.append(f"required:{f}")

            # Ranges
            for rr in range_rules:
                v = typed.get(rr.field)
                if v is None:
                    continue
                if rr.min is not None and v < rr.min:
                    reasons.append(f"range_min:{rr.field}")
                if rr.max is not None and v > rr.max:
                    reasons.append(f"range_max:{rr.field}")

            # Allowed
            for ar in allowed_rules:
                v = typed.get(ar.field)
                if v is None:
                    continue
                vs = str(v).strip().lower()
                if vs not in {x.lower() for x in ar.allowed}:
                    reasons.append(f"allowed:{ar.field}")

            # Cross rules
            for cr in cross_rules:
                ok = True
                try:
                    ok = bool(cr.fn(typed))
                except Exception:
                    ok = False
                if not ok:
                    reasons.append(f"cross:{cr.name}")

            if reasons:
                bad += 1
                rh = row_hash(raw)
                reject_rows.append(
                    (
                        spec.name,
                        source_file,
                        rownum,
                        rh,
                        "|".join(reasons),
                        json.dumps(raw, ensure_ascii=False),
                    )
                )
            else:
                good += 1
                vals = [typed[c] for c in final_cols]
                if needs_pk_dup_param:
                    # append pk again for the NOT EXISTS (...) = ?
                    vals.append(typed[pk_col])
                good_rows.append(vals)

            # Flush batches
            if len(good_rows) >= batch_size:
                cur.executemany(insert_final_sql, good_rows)
                conn.commit()
                good_rows = []

            if len(reject_rows) >= batch_size:
                cur.executemany(insert_reject_sql, reject_rows)
                conn.commit()
                reject_rows = []

        if good_rows:
            cur.executemany(insert_final_sql, good_rows)
            conn.commit()

        if reject_rows:
            cur.executemany(insert_reject_sql, reject_rows)
            conn.commit()

        print(f"transform_dataset ✅ dataset={spec.name} total={total} good={good} bad={bad}")
    finally:
        conn.close()
