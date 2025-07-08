import json
from typing import Optional, TypeVar
from pydantic import BaseModel
from zavod.context import Context
from zavod.db import get_engine
from zavod.stateful.model import extraction_table
from sqlalchemy import insert, select, update
from datetime import datetime
from hashlib import sha1

T = TypeVar("T", bound=BaseModel)


def extract_items(
    context: Context, key: str, raw_data: T, source_url: str
) -> Optional[T]:
    """Add raw data to the extraction table and return accepted extracted data if hash matches, else empty list."""
    raw_data_dump = raw_data.model_dump()
    # TODO: might need to sort arrays too
    raw_data_json = json.dumps(raw_data_dump, sort_keys=True)
    data_hash = sha1(raw_data_json.encode("utf-8")).hexdigest()
    dataset = context.dataset.name
    engine = get_engine()
    with engine.begin() as conn:
        select_stmt = select(extraction_table).where(
            extraction_table.c.key == key, extraction_table.c.deleted_at == None
        )
        row = conn.execute(select_stmt).mappings().first()
        if row is None:
            # First insert
            context.log.debug("Extraction key miss", key=key)
            ins = insert(extraction_table).values(
                key=key,
                dataset=dataset,
                schema=type(raw_data).model_json_schema(),
                source_url=source_url,
                accepted=False,
                raw_data=raw_data_dump,
                raw_data_hash=data_hash,
                extracted_data=raw_data_dump,
                last_seen_version=context.version.id,
                created_at=datetime.utcnow(),
            )
            conn.execute(ins)
            return None
        # Row exists
        if row["raw_data_hash"] == data_hash:
            context.log.debug(
                "Extraction key hit, hash matches", key=key, accepted=row["accepted"]
            )
            if row["accepted"]:
                return type(raw_data).model_validate(row["extracted_data"])
            else:
                return None
        # Hash differs, mark old row as deleted and insert new row
        context.log.debug("Extraction key hit, hash differs", key=key)
        now = datetime.utcnow()
        conn.execute(
            update(extraction_table)
            .where(extraction_table.c.id == row["id"])
            .values(deleted_at=now, modified_at=now)
        )
        ins = insert(extraction_table).values(
            key=key,
            dataset=dataset,
            schema=type(raw_data).model_json_schema(),
            source_url=source_url,
            accepted=False,
            raw_data=raw_data_dump,
            raw_data_hash=data_hash,
            extracted_data=raw_data_dump,
            last_seen_version=context.version.id,
            created_at=now,
        )
        conn.execute(ins)
        return None
