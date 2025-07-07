import json
from typing import Type, TypeVar, List
from pydantic import BaseModel, parse_obj_as
from zavod.db import get_engine
from zavod.stateful.model import extraction_table
from sqlalchemy import insert, select, update
from datetime import datetime
from hashlib import sha1

T = TypeVar("T", bound=BaseModel)


def extract_items(
    context, key: str, raw_data: List[T], source_url: str, model_type: Type[T]
) -> List[T]:
    """Add raw data to the extraction table and return accepted extracted data if hash matches, else empty list."""
    # Compute hash of the raw_data
    raw_data_json = json.dumps([item.model_dump() for item in raw_data], sort_keys=True)
    data_hash = sha1(raw_data_json.encode("utf-8")).hexdigest()
    dataset = context.dataset.name
    engine = get_engine()
    with engine.begin() as conn:
        stmt = select(extraction_table).where(
            extraction_table.c.key == key, extraction_table.c.deleted_at == None
        )
        row = conn.execute(stmt).mappings().first()
        if row is None:
            # First insert
            ins = insert(extraction_table).values(
                key=key,
                dataset=dataset,
                schema=model_type.model_json_schema(),
                source_url=source_url,
                accepted=False,
                raw_data=json.loads(raw_data_json),
                extracted_data_hash=data_hash,
                extracted_data=[],
                last_seen_version=datetime.utcnow().isoformat(),
                created_at=datetime.utcnow(),
            )
            conn.execute(ins)
            return []
        # Row exists
        if row["extracted_data_hash"] == data_hash:
            if row["accepted"]:
                # Return accepted extracted data
                return parse_obj_as(List[model_type], row["extracted_data"])
            else:
                return []
        # Hash differs, mark old row as deleted and insert new row
        now = datetime.utcnow()
        conn.execute(
            update(extraction_table)
            .where(extraction_table.c.id == row["id"])
            .values(deleted_at=now, modified_at=now)
        )
        ins = insert(extraction_table).values(
            key=key,
            dataset=dataset,
            schema=model_type.model_json_schema(),
            source_url=source_url,
            accepted=False,
            raw_data=json.loads(raw_data_json),
            extracted_data_hash=data_hash,
            extracted_data=[],
            last_seen_version=now.isoformat(),
            created_at=now,
        )
        conn.execute(ins)
        return []
