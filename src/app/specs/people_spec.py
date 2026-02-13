# src/app/specs/people_spec.py
from __future__ import annotations

from app.transform_framework import (
    DatasetSpec,
    FieldRule,
    IndexSpec,
)

PEOPLE_SPEC = DatasetSpec(
    name="people",
    stg_table="dbo.stage_people",
    final_table="dbo.people_typed",
    fields=[
        FieldRule(field="person_id", source="person_id", cast="int", required=True),
        FieldRule(field="full_name", source="full_name", cast="str", required=True),
        FieldRule(field="created_at", source="created_at", cast="date", required=False),
    ],
    indexes=[
        IndexSpec(
            name="IX_people_typed_person_id",
            columns=["person_id"],
            unique=True,
        ),
        IndexSpec(
            name="IX_people_typed_full_name",
            columns=["full_name"],
            unique=False,
        ),
    ],
)
