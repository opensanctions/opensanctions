from datetime import datetime
from hashlib import sha1
from typing import Any, Dict, Generic, Optional, Type, TypeVar, cast

import orjson
from normality import slugify
from pydantic import BaseModel, JsonValue
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode
from pydantic_core import CoreSchema
from sqlalchemy import func, insert, not_, select, update
from sqlalchemy.engine import Connection
from sqlalchemy.sql import Select

from zavod.context import Context
from zavod.db import get_engine
from zavod.stateful.model import extraction_table

ModelType = TypeVar("ModelType", bound=BaseModel)


class SchemaGenerator(GenerateJsonSchema):
    def generate(
        self, schema: CoreSchema, mode: JsonSchemaMode = "validation"
    ) -> Dict[str, Any]:
        json_schema = super().generate(schema, mode=mode)
        json_schema["$schema"] = self.schema_dialect
        return json_schema


class Review(BaseModel, Generic[ModelType]):
    """
    A review is the smallest unit of data that's convenient to extract data from,
    as well as information about the source data, whether it's been accepted, and by whom.
    """

    id: Optional[int] = None
    key: str
    dataset: str
    extraction_schema: JsonValue
    source_value: str
    source_content_type: str
    source_label: str
    source_url: Optional[str]
    orig_extraction_data: ModelType
    orig_extraction_data_hash: str
    extracted_data: ModelType
    accepted: bool
    last_seen_version: str
    modified_at: datetime
    modified_by: str
    deleted_at: Optional[datetime] = None

    @classmethod
    def load(
        cls, conn: Connection, data_model: Type[ModelType], stmt: Select
    ) -> Optional["Review[ModelType]"]:
        res = conn.execute(stmt)
        rows = list(res.fetchall())
        assert len(rows) <= 1
        if rows == []:
            return None
        review = cls.model_validate(rows[0]._mapping)
        review.orig_extraction_data = data_model.model_validate(
            rows[0]._mapping["orig_extraction_data"]
        )
        review.extracted_data = data_model.model_validate(
            rows[0]._mapping["extracted_data"]
        )
        return review

    @classmethod
    def by_key(
        cls, conn: Connection, data_model: Type[ModelType], dataset: str, key: str
    ) -> Optional["Review[ModelType]"]:
        select_stmt = select(extraction_table).where(
            extraction_table.c.dataset == dataset,
            extraction_table.c.key == key,
            extraction_table.c.deleted_at.is_(None),
        )
        return cls.load(conn, data_model, select_stmt)

    def save(self, conn: Connection) -> None:
        if self.id:
            conn.execute(
                update(extraction_table)
                .where(extraction_table.c.id == self.id)
                .values(deleted_at=datetime.now())
            )
        ins = insert(extraction_table).values(
            key=self.key,
            dataset=self.dataset,
            schema=self.extraction_schema,
            source_value=self.source_value,
            source_content_type=self.source_content_type,
            source_label=self.source_label,
            source_url=self.source_url,
            orig_extraction_data=self.orig_extraction_data.model_dump(),
            orig_extraction_data_hash=self.orig_extraction_data_hash,
            extracted_data=self.extracted_data.model_dump(),
            accepted=self.accepted,
            last_seen_version=self.last_seen_version,
            modified_at=self.modified_at,
            modified_by=self.modified_by,
        )
        conn.execute(ins)

    def update_version(self, conn: Connection, version_id: str) -> None:
        """Update the last_seen_version without adding a historical row."""
        assert self.id is not None
        conn.execute(
            update(extraction_table)
            .where(extraction_table.c.id == self.id)
            .values(last_seen_version=version_id)
        )

    @classmethod
    def count_unaccepted(cls, conn: Connection, dataset: str, version_id: str) -> int:
        select_stmt = select(func.count(extraction_table.c.id)).where(
            extraction_table.c.dataset == dataset,
            extraction_table.c.last_seen_version == version_id,
            not_(extraction_table.c.accepted),
        )
        return cast(int, conn.execute(select_stmt).scalar_one())


def sort_arrays_in_value(value: JsonValue) -> JsonValue:
    """Recursively sort arrays within a json-serializable value to ensure consistent ordering."""
    if isinstance(value, list):
        # Recursively sort array elements and then sort the array itself
        return sorted(
            [sort_arrays_in_value(item) for item in value],
            key=lambda x: orjson.dumps(x, option=orjson.OPT_SORT_KEYS),
        )
    elif isinstance(value, dict):
        # Recursively handle nested objects
        return {k: sort_arrays_in_value(v) for k, v in value.items()}
    return value


def hash_data(data: BaseModel) -> str:
    """
    Generate a consistent SHA-1 hash of orjson-serializable data.
    Arrays within the data are sorted to ensure the same hash regardless of order.
    """
    raw_data_dump = data.model_dump()
    # Sort arrays recursively to ensure consistent ordering
    sorted_data = sort_arrays_in_value(raw_data_dump)
    # Sort dictionary keys and convert to orjson
    raw_data_json = orjson.dumps(sorted_data, option=orjson.OPT_SORT_KEYS)
    return sha1(raw_data_json).hexdigest()


def get_accepted_data(
    context: Context,
    key: str | list[str],
    source_value: str,
    source_content_type: str,
    source_label: str,
    source_url: Optional[str],
    orig_extraction_data: ModelType,
    default_accepted: bool = False,
) -> Optional[ModelType]:
    """
    Add automatically extracted data for review and
    return extracted data if it's marked as accepted.

    Args:
        key: The key will be slugified. Should be unique within the dataset,
             and as consistent as possible between runs.
        source_label: Where the source value is from for reviewer context
             e.g. "Banking Organization field in CSV" or "Screenshot of PDF page"
        source_value: The value of the original text, or a url to an archived original
             to be used as evidence by the reviewer and for provenance if we're challenged.
        source_url: The url where source_value was fetched from.
        orig_extraction_data: An instance of a pydantic model of data extracted
             from the source. Any changes to the data will be validated against
             this model.
    """
    key_slug = slugify(key)
    assert key_slug is not None
    data_hash = hash_data(orig_extraction_data)
    dataset = context.dataset.name
    schema = type(orig_extraction_data).model_json_schema(
        schema_generator=SchemaGenerator
    )
    engine = get_engine()
    now = datetime.now()
    with engine.begin() as conn:
        review = Review[ModelType].by_key(
            conn, type(orig_extraction_data), dataset, key_slug
        )
        if review is None:
            # First insert
            context.log.debug("Extraction key miss", key=key)
            review = Review[ModelType](
                key=key_slug,
                dataset=dataset,
                extraction_schema=schema,
                source_value=source_value,
                source_content_type=source_content_type,
                source_label=source_label,
                source_url=source_url,
                orig_extraction_data=orig_extraction_data,
                orig_extraction_data_hash=data_hash,
                extracted_data=orig_extraction_data,
                accepted=default_accepted,
                last_seen_version=context.version.id,
                modified_at=now,
                modified_by="zavod",
            )
            review.save(conn)

        # Row exists
        elif review.orig_extraction_data_hash == data_hash:
            context.log.debug("Extraction key hit, hash matches", key=key)
            # Update last_seen_version to current version
            review.update_version(conn, context.version.id)

        # Hash differs, mark old row as deleted and insert new row
        else:
            context.log.debug("Extraction key hit, hash differs", key=key_slug)
            review.extraction_schema = type(orig_extraction_data).model_json_schema()
            review.source_value = source_value
            review.source_content_type = source_content_type
            review.source_label = source_label
            review.source_url = source_url
            review.orig_extraction_data = orig_extraction_data
            review.orig_extraction_data_hash = data_hash
            review.extracted_data = orig_extraction_data
            review.accepted = default_accepted
            review.last_seen_version = context.version.id
            review.modified_at = now
            review.modified_by = "zavod"
            review.save(conn)

        return review.extracted_data if review.accepted else None


def assert_all_accepted(context: Context) -> None:
    """
    Raise an exception with the number of unacpeted itemsif any extraction
    entries for the current dataset and version are not accepted.
    """
    engine = get_engine()
    with engine.begin() as conn:
        count = Review.count_unaccepted(conn, context.dataset.name, context.version.id)
        if count > 0:
            raise Exception(
                (
                    f"There are {count} unaccepted items for dataset "
                    f"{context.dataset.name} and version {context.version.id}"
                )
            )
