from __future__ import annotations

import argparse

from app.config import get_db_config
from app.exporters.people_exporter import export_people_csv
from app.importers.people_importer import import_people_csv
from app.loaders.csv_loader import load_csv
from app.migrations.runner import apply_migrations
from app.people_repo import (
    add_person,
    delete_person,
    find_people,
    get_person,
    list_people,
    update_person_name,
)
from app.promoters.people_promoter import promote_people
from app.raw_repo import count_raw_orders
from app.rejects_repo import count_rejects, list_rejects
from app.specs.people_spec import PEOPLE_SPEC
from app.sql_utils import count_table
from app.stage_repo import clear_stage_people
from app.table_tools import truncate_table
from app.transform_framework import transform_dataset
from app.transform_schema import ensure_final_table_from_spec
from app.db import get_conn
from app.exporters.rejects_exporter import export_rejects_jsonl, export_rejects_csv






def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ops")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ping", help="Smoke test command")
    sub.add_parser("show_config", help="Print loaded DB config (sanity check)")
    sub.add_parser("migrate", help="Apply pending SQL migrations")

    sub.add_parser("count_raw", help="Count rows in dbo.raw_orders")

    # people CRUD
    p_add = sub.add_parser("add_person", help="Add a person to dbo.people")
    p_add.add_argument("--name", required=True, help="Full name")

    p_list = sub.add_parser("list_people", help="List people from dbo.people")
    p_list.add_argument("--top", type=int, default=20, help="How many rows to show")

    p_find = sub.add_parser("find_person", help="Find people by name substring")
    p_find.add_argument("--like", required=True, help="Substring to search for")
    p_find.add_argument("--top", type=int, default=20, help="How many rows to show")

    p_get = sub.add_parser("get_person", help="Get one person by id")
    p_get.add_argument("--id", type=int, required=True, help="person_id to fetch")

    p_upd = sub.add_parser("update_person", help="Update a person's name by id")
    p_upd.add_argument("--id", type=int, required=True, help="person_id to update")
    p_upd.add_argument("--name", required=True, help="New full name")

    p_del = sub.add_parser("delete_person", help="Delete person by id")
    p_del.add_argument("--id", type=int, required=True, help="person_id to delete")

    # export / import
    p_exp = sub.add_parser("export_people", help="Export dbo.people to CSV")
    p_exp.add_argument("--out", required=True, help="Output CSV path (e.g. .\\exports\\people.csv)")
    p_exp.add_argument("--top", type=int, default=0, help="If >0, export only top N")

    p_imp = sub.add_parser("import_people", help="Import dbo.people from CSV")
    p_imp.add_argument("--in", dest="in_path", required=True, help="Input CSV path (e.g. .\\exports\\people.csv)")

    # generic load csv -> staging
    p_load = sub.add_parser("load_csv", help="Load a CSV into a staging table (NVARCHAR)")
    p_load.add_argument("--csv", dest="csv_path", required=True, help="Path to CSV file")
    p_load.add_argument("--table", required=True, help="Target table (e.g. dbo.stage_people)")
    p_load.add_argument("--batch-size", type=int, default=2000, help="Rows per batch commit")
    p_load.add_argument("--drop-create", action="store_true", help="Drop & recreate table before load (requires confirm)")
    p_load.add_argument("--truncate", action="store_true", help="Truncate table before load (requires confirm)")
    p_load.add_argument("--delimiter", default=",", help="CSV delimiter (default ,)")
    p_load.add_argument("--quotechar", default='"', help='CSV quote char (default ")')
    p_load.add_argument("--skiprows", type=int, default=0, help="Rows to skip before header")
    p_load.add_argument("--match-mode", choices=["strict", "set"], default="strict", help="Column match mode when not recreating")
    p_load.add_argument("--require-confirm", dest="confirm", default=None, help='Confirmation string, e.g. "DROP_CREATE dbo.stage_people"')

    # generic table tools
    p_ct = sub.add_parser("count_table", help="Count rows in any table")
    p_ct.add_argument("--table", required=True, help="Table name (e.g. dbo.stage_people)")

    p_tr = sub.add_parser("truncate_table", help="TRUNCATE a table (requires confirmation)")
    p_tr.add_argument("--table", required=True, help="Full table name, e.g. dbo.stage_people")
    p_tr.add_argument("--require-confirm", dest="confirm", required=True, help='Must equal: "TRUNCATE <table>"')

    sub.add_parser("clear_stage_people", help="Delete all rows from dbo.stage_people")

    # old promoter path
    p_prom = sub.add_parser("promote_people", help="Promote dbo.stage_people -> dbo.people_typed (with rejects)")
    p_prom.add_argument("--from", dest="from_table", default="dbo.stage_people", help="Staging table (default dbo.stage_people)")

    # spec-based typed table + transform
    sub.add_parser("ensure_people_final", help="Create/ensure dbo.people_typed from PEOPLE spec")

    p_tf = sub.add_parser("transform_people", help="Transform dbo.stage_people -> dbo.people_typed + dataset_rejects")
    p_tf.add_argument("--source-file", default=None, help="Optional source filename to store in dataset_rejects")
    p_tf.add_argument("--truncate-final", action="store_true", help="TRUNCATE final table before insert")
    p_tf.add_argument("--truncate-rejects", action="store_true", help="Clear rejects for this dataset before insert")

    # NEW: rejects inspection
    p_rc = sub.add_parser("rejects_count", help="Count rejects for a dataset")
    p_rc.add_argument("--dataset", required=True, help="Dataset name (e.g. people)")

    p_rs = sub.add_parser("rejects_show", help="Show recent rejects for a dataset")
    p_rs.add_argument("--dataset", required=True, help="Dataset name (e.g. people)")
    p_rs.add_argument("--top", type=int, default=20, help="How many rows to show")
    sub.add_parser("db_ping", help="Connect to SQL Server and run SELECT 1")

    p_rj = sub.add_parser("rejects_export", help="Export <dataset>_rejects table to JSONL")
    p_rj.add_argument("--dataset", required=True, help="Dataset name (e.g. people)")
    p_rj.add_argument("--out", required=True, help="Output path (e.g. .\\exports\\people_rejects.jsonl)")
    p_rj.add_argument("--top", type=int, default=0, help="If >0, export only top N")

    p_rc = sub.add_parser("rejects_export_csv", help="Export <dataset>_rejects table to CSV")
    p_rc.add_argument("--dataset", required=True, help="Dataset name (e.g. people)")
    p_rc.add_argument("--out", required=True, help="Output path (e.g. .\\exports\\people_rejects.csv)")
    p_rc.add_argument("--top", type=int, default=0, help="If >0, export only top N")



    return p


def _print_people_rows(rows: list[tuple[int, str, str]]) -> None:
    if not rows:
        print("people ✅ empty")
        return
    print("person_id | full_name | created_at")
    print("-" * 60)
    for pid, name, created in rows:
        print(f"{pid} | {name} | {created}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "ping":
        print("pong ✅")
        return 0

    if args.cmd == "show_config":
        cfg = get_db_config()
        print(cfg)
        return 0

    if args.cmd == "migrate":
        n = apply_migrations()
        print(f"migrate ✅ applied={n}")
        return 0

    if args.cmd == "count_raw":
        n = count_raw_orders()
        print(f"raw_orders ✅ count={n}")
        return 0

    if args.cmd == "add_person":
        new_id = add_person(args.name)
        print(f"people ✅ inserted person_id={new_id}")
        return 0

    if args.cmd == "list_people":
        _print_people_rows(list_people(args.top))
        return 0

    if args.cmd == "find_person":
        rows = find_people(args.like, args.top)
        if not rows:
            print("people ✅ no matches")
            return 0
        _print_people_rows(rows)
        return 0

    if args.cmd == "get_person":
        row = get_person(args.id)
        if row is None:
            print(f"people ✅ not found id={args.id}")
            return 0
        pid, name, created = row
        print("person_id | full_name | created_at")
        print("-" * 60)
        print(f"{pid} | {name} | {created}")
        return 0

    if args.cmd == "update_person":
        if args.id <= 0:
            raise SystemExit("id must be a positive integer")
        updated = update_person_name(args.id, args.name)
        print(f"people ✅ updated={updated} id={args.id}")
        return 0

    if args.cmd == "delete_person":
        if args.id <= 0:
            raise SystemExit("id must be a positive integer")
        deleted = delete_person(args.id)
        print(f"people ✅ deleted={deleted} id={args.id}")
        return 0

    if args.cmd == "export_people":
        top = args.top if args.top and args.top > 0 else None
        path = export_people_csv(args.out, top=top)
        print(f"people ✅ exported={path}")
        return 0

    if args.cmd == "import_people":
        stats = import_people_csv(args.in_path)
        print(f"people ✅ import read={stats['read']} inserted={stats['inserted']} skipped={stats['skipped']}")
        return 0

    if args.cmd == "load_csv":
        load_csv(
            csv_path=args.csv_path,
            table=args.table,
            batch_size=args.batch_size,
            drop_and_recreate=args.drop_create,
            truncate=args.truncate,
            delimiter=args.delimiter,
            quotechar=args.quotechar,
            skiprows=args.skiprows,
            match_mode=args.match_mode,
            confirm=args.confirm,
        )
        return 0

    if args.cmd == "count_table":
        n = count_table(args.table)
        print(f"table ✅ {args.table} count={n}")
        return 0

    if args.cmd == "truncate_table":
        before = truncate_table(table=args.table, confirm=args.confirm)
        print(f"table ✅ truncated={args.table} rows_before={before}")
        return 0

    if args.cmd == "clear_stage_people":
        n = clear_stage_people()
        print(f"stage_people ✅ cleared={n}")
        return 0

    if args.cmd == "promote_people":
        good, bad = promote_people(args.from_table)
        print(f"people ✅ promoted good={good} rejected={bad}")
        return 0

    if args.cmd == "ensure_people_final":
        ensure_final_table_from_spec(PEOPLE_SPEC)
        return 0

    if args.cmd == "transform_people":
        transform_dataset(
            PEOPLE_SPEC,
            source_file=args.source_file,
            truncate_final=bool(args.truncate_final),
            truncate_rejects=bool(args.truncate_rejects),
        )
        return 0

    if args.cmd == "rejects_count":
        n = count_rejects(args.dataset)
        print(f"rejects ✅ dataset={args.dataset} count={n}")
        return 0

    if args.cmd == "rejects_show":
        rows = list_rejects(args.dataset, top=args.top)
        if not rows:
            print(f"rejects ✅ dataset={args.dataset} empty")
            return 0

        for r in rows:
            print("-" * 60)
            print(f"row_num={r['row_num']} source_file={r['source_file']}")
            print(f"reasons={r['reasons']}")
            print(f"raw={r['raw']}")
        return 0
    
    if args.cmd == "db_ping":
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            row = cur.fetchone()
            conn.commit()

        print(f"db ✅ ok={row[0]}")
        return 0
    
    if args.cmd == "rejects_export":
        path = export_rejects_jsonl(dataset=args.dataset, out_path=args.out, top=args.top)
        print(f"rejects ✅ exported={path}")
        return 0
    
    if args.cmd == "rejects_export_csv":
        path = export_rejects_csv(dataset=args.dataset, out_path=args.out, top=args.top)
        print(f"rejects ✅ exported_csv={path}")
        return 0

    
    parser.print_help()
    return 2








