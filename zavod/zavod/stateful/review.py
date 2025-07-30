from datetime import datetime, timezone
from typing import Any, Dict, Generic, Optional, Type, TypeVar

from normality import slugify
from pydantic import BaseModel, JsonValue, PrivateAttr
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode
from pydantic_core import CoreSchema
from sqlalchemy import func, insert, not_, select, update
from sqlalchemy.engine import Connection
from sqlalchemy.sql import Select
from logging import getLogger

from zavod.context import Context
from zavod.db import get_engine
from zavod.stateful.model import review_table

log = getLogger(__name__)

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
    A review is the smallest unit of data that's convenient to extract data from and review,
    along with info about the source data, whether it's been accepted, and by who.
    """

    id: Optional[int] = None
    key: str
    dataset: str
    extraction_schema: JsonValue
    source_value: str
    source_content_type: str
    source_label: str
    source_url: Optional[str]
    data_model: Type[ModelType]
    model_version: int
    _orig_extraction_data: JsonValue = PrivateAttr()
    _extracted_data: JsonValue = PrivateAttr()
    accepted: bool
    last_seen_version: str
    modified_at: datetime
    modified_by: str
    deleted_at: Optional[datetime] = None

    @property
    def orig_extraction_data(self) -> ModelType:
        return self.data_model.model_validate(self._orig_extraction_data)

    @orig_extraction_data.setter
    def orig_extraction_data(self, value: ModelType) -> None:
        self._orig_extraction_data = value.model_dump()

    @property
    def extracted_data(self) -> ModelType:
        return self.data_model.model_validate(self._extracted_data)

    @extracted_data.setter
    def extracted_data(self, value: ModelType) -> None:
        self._extracted_data = value.model_dump()

    def __init__(self, **data):  # type: ignore
        super().__init__(**data)
        if "_orig_extraction_data" in data:
            self._orig_extraction_data = data.pop("_orig_extraction_data")
        else:
            self._orig_extraction_data = data.pop("orig_extraction_data").model_dump()
        if "_extracted_data" in data:
            self._extracted_data = data.pop("_extracted_data")
        else:
            self._extracted_data = data.pop("extracted_data").model_dump()

    @classmethod
    def load(
        cls,
        conn: Connection,
        data_model: Type[ModelType],
        stmt: Select,  # type: ignore
    ) -> Optional["Review[ModelType]"]:
        res = conn.execute(stmt)
        rows = list(res.fetchall())
        assert len(rows) <= 1
        if rows == []:
            return None
        review_data = dict(rows[0]._mapping)
        review_data["_orig_extraction_data"] = review_data.pop("orig_extraction_data")
        review_data["_extracted_data"] = review_data.pop("extracted_data")
        review_data["data_model"] = data_model
        review = cls.model_validate(review_data)
        return review

    @classmethod
    def by_key(
        cls, conn: Connection, data_model: Type[ModelType], dataset: str, key: str
    ) -> Optional["Review[ModelType]"]:
        select_stmt = select(review_table).where(
            review_table.c.dataset == dataset,
            review_table.c.key == key,
            review_table.c.deleted_at.is_(None),
        )
        return cls.load(conn, data_model, select_stmt)

    def save(self, conn: Connection) -> None:
        if self.id:
            conn.execute(
                update(review_table)
                .where(review_table.c.id == self.id)
                .values(deleted_at=datetime.now(timezone.utc))
            )
        ins = insert(review_table).values(
            key=self.key,
            dataset=self.dataset,
            extraction_schema=self.extraction_schema,
            source_value=self.source_value,
            source_content_type=self.source_content_type,
            source_label=self.source_label,
            source_url=self.source_url,
            model_version=self.model_version,
            orig_extraction_data=self._orig_extraction_data,
            extracted_data=self._extracted_data,
            accepted=self.accepted,
            last_seen_version=self.last_seen_version,
            modified_at=self.modified_at,
            modified_by=self.modified_by,
        )
        conn.execute(ins)

    def update_version(self, conn: Connection, version_id: str) -> None:
        """Update the last_seen_version without adding a historical row."""
        assert self.id is not None
        self.last_seen_version = version_id
        conn.execute(
            update(review_table)
            .where(review_table.c.id == self.id)
            .values(last_seen_version=self.last_seen_version)
        )

    @classmethod
    def count_unaccepted(cls, conn: Connection, dataset: str, version_id: str) -> int:
        select_stmt = select(func.count(review_table.c.id)).where(
            review_table.c.dataset == dataset,
            review_table.c.last_seen_version == version_id,
            not_(review_table.c.accepted),
        )
        return conn.execute(select_stmt).scalar_one()


def request_review(
    context: Context,
    key: str | list[str],
    source_value: str,
    source_data_hash: str,
    source_content_type: str,
    source_label: str,
    source_url: Optional[str],
    orig_extraction_data: ModelType,
    model_version: int,
    default_accepted: bool = False,
) -> Optional[Review[ModelType]]:
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
    dataset = context.dataset.name
    schema = type(orig_extraction_data).model_json_schema(
        schema_generator=SchemaGenerator
    )
    now = datetime.now(timezone.utc)
    with get_engine().begin() as conn:
        review = Review.by_key(conn, type(orig_extraction_data), dataset, key_slug)
        if review is None:
            # First insert
            context.log.debug("Requesting review", key=key)
            review = Review(
                key=key_slug,
                dataset=dataset,
                extraction_schema=schema,
                source_value=source_value,
                source_data_hash=source_data_hash,
                source_content_type=source_content_type,
                source_label=source_label,
                source_url=source_url,
                data_model=type(orig_extraction_data),
                model_version=model_version,
                orig_extraction_data=orig_extraction_data,
                extracted_data=orig_extraction_data,
                accepted=default_accepted,
                last_seen_version=context.version.id,
                modified_at=now,
                modified_by="zavod",
            )  # type: ignore
            review.save(conn)

        # We're re-requesting review - likely because the model version > min_model_version.
        # Mark old row as deleted and insert new row
        else:
            context.log.debug("Re-requesting review", key=key_slug)
            review.extraction_schema = schema
            review.source_value = source_value
            review.source_data_hash = source_data_hash
            review.source_content_type = source_content_type
            review.source_label = source_label
            review.source_url = source_url
            review.model_version = model_version
            review.orig_extraction_data = orig_extraction_data
            review.extracted_data = orig_extraction_data
            review.accepted = default_accepted
            review.last_seen_version = context.version.id
            review.modified_at = now
            review.modified_by = "zavod"
            review.save(conn)

        return review


def get_review(
    context: Context,
    data_model: Type[ModelType],
    key: str,
    min_crawler_version: int,
    source_data_hash: str,
) -> Optional[Review[ModelType]]:
    """
    Get a review for a given key if it exists and its model is up to date.
    Returned reviews will have had their last_seen_version updated to the current version.
    """
    key_slug = slugify(key)
    assert key_slug is not None
    with get_engine().begin() as conn:
        review = Review[ModelType].by_key(
            conn, data_model, context.dataset.name, key_slug
        )

        if review is None:
            context.log.debug("Review not found", key=key_slug)
            return None

        if review.crawler_version < min_crawler_version:
            context.log.debug(
                "Review model version is too old",
                key=key_slug,
                min_crawler_version=min_crawler_version,
                crawler_version=review.crawler_version,
            )
            return None

        if review.accepted and review.source_data_hash != source_data_hash:
            context.log.warning("Review source data hash mismatch", key=key_slug)

        context.log.debug("Review found, updating last_seen_version", key=key_slug)
        review.update_version(conn, context.version.id)
        return review


def assert_all_accepted(context: Context, raise_on_unaccepted=True) -> None:
    """
    Raise an exception with the number of unaccepted items if any extraction
    entries for the current dataset and version are not accepted.
    """
    with get_engine().begin() as conn:
        count = Review.count_unaccepted(conn, context.dataset.name, context.version.id)
        if count > 0:
            message = (
                f"There are {count} unaccepted items for dataset "
                f"{context.dataset.name} and version {context.version.id}"
            )
            if raise_on_unaccepted:
                raise Exception(message)
            else:
                context.log.warning(message)
