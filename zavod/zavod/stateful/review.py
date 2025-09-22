from abc import ABC
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar
from logging import getLogger

from normality import slugify
from pydantic import BaseModel, JsonValue, PrivateAttr
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode
from pydantic_core import CoreSchema
from rigour.mime.types import HTML
from rigour.text import text_hash
from sqlalchemy import func, insert, not_, select, update
from sqlalchemy.engine import Connection
from sqlalchemy.sql import Select
import orjson
from lxml.html import HtmlElement, fromstring

from zavod.context import Context
from zavod.stateful.model import review_table
from zavod import helpers as h

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
    extraction_checksum: Optional[str]
    extraction_schema: JsonValue
    source_value: str
    source_mime_type: str
    source_label: str
    source_url: Optional[str]
    data_model: Type[ModelType]
    crawler_version: int
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
            extraction_checksum=self.extraction_checksum,
            extraction_schema=self.extraction_schema,
            source_value=self.source_value,
            source_mime_type=self.source_mime_type,
            source_label=self.source_label,
            source_url=self.source_url,
            crawler_version=self.crawler_version,
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


class ExtractionConfig(Generic[ModelType]):
    checksum: str
    data_model: Type[ModelType]


class BackfillExtractionConfig(ExtractionConfig[ModelType]):
    def __init__(self, data_model: Type[ModelType]):
        self.data_model = data_model
        self.checksum = "backfilled"


class LLMExtractionConfig(ExtractionConfig[ModelType]):
    def __init__(self, data_model: Type[ModelType], llm_model: str, prompt: str):
        self.data_model = data_model
        self.llm_model = llm_model
        self.checksum = text_hash(llm_model + prompt)
        self.prompt = prompt


class SourceValue(ABC):
    key_parts: str | List[str]
    mime_type: str
    label: str
    url: Optional[str]
    value_string: str

    def matches(self, review: Review[ModelType]) -> bool:
        raise NotImplementedError


class HtmlSourceValue(SourceValue):
    html: str
    element: HtmlElement

    def __init__(
        self,
        key_parts: str | List[str],
        mime_type: str,
        label: str,
        url: Optional[str],
        value_string: str,
        element: HtmlElement,
    ):
        """
        Args:
            key_parts: The key parts will be slugified and shorted with a hash of all the
                parts if the slug would be too long.

                Should be unique within the dataset, and as consistent as possible between runs.
            source_value: The value of the original text, or a url to an archived original
                to be used as evidence by the reviewer and for provenance if we're challenged.
            source_mime_type: The mime type of the source value. Useful to know how to display
                the source value to the reviewer.
            source_label: Used to indicate the context of the source value to the reviewer,
                e.g. "Banking Organization field in CSV" or "Screenshot of PDF page"
            source_url: The url where source_value was fetched from.
            value_string: The string value of the source value.
        """
        self.key_parts = key_parts
        self.mime_type = mime_type
        self.label = label
        self.url = url
        self.value_string = value_string
        self.element = element

    def matches(self, review: Review[ModelType]) -> bool:
        assert review.source_mime_type == HTML, review.source_mime_type
        seen_element = fromstring(review.source_value)
        return h.html.element_text_hash(seen_element) == h.html.element_text_hash(
            self.element
        )


class SourceObservation(Generic[ModelType]):
    def __init__(self, review: Optional[Review[ModelType]], should_extract: bool):
        self.review = review
        self.should_extract = should_extract


def review_key(parts: str | List[str]) -> str:
    """Generates a unique key for a given string of party names.
    If the slug would be longer than 255 chars, we include a truncated hash of the
    string with part of the slug to ensure it's consistent, unique, and short enough.
    """
    slug = slugify(parts)
    assert slug is not None
    # Hardcoding based on model.KEY_LEN to prevent inadvertent key changes
    if len(slug) <= 255:
        return slug
    else:
        hash = sha1(slug.encode("utf-8")).hexdigest()
        return f"{slug[:80]}-{hash[:10]}"


def should_review(
    review: Optional[Review[ModelType]], extraction_result: ModelType
) -> bool:
    """
    Determine if a review should be requested for a given extraction result.
    """
    if review is None:
        return True
    return model_hash(extraction_result) != model_hash(review.orig_extraction_data)


def request_review(
    context: Context,
    source_value: SourceValue,
    extraction_config: ExtractionConfig[ModelType],
    orig_extraction_data: ModelType,
    crawler_version: int,
    default_accepted: bool = False,
) -> Review[ModelType]:
    """
    Add automatically extracted data for review and
    return extracted data if it's marked as accepted.

    Args:
        context: The runner context with dataset metadata.
        source_value: The source value for the extracted data.
        extraction_config: The configuration for extracting the data.
        orig_extraction_data: An instance of a pydantic model of data extracted
            from the source. Any reviewer changes to the data will be validated against
            this model.
        crawler_version: A version number a crawler can use as a checkpoint for changes
            requiring re-extraction and/or re-review.
            Useful e.g. when breaking model changes are made.
        default_accepted: Whether the data should be marked as accepted by default.
    """
    key_slug = review_key(source_value.key_parts)
    assert key_slug is not None
    dataset = context.dataset.name

    review = Review.by_key(context.conn, type(orig_extraction_data), dataset, key_slug)

    schema = type(orig_extraction_data).model_json_schema(
        schema_generator=SchemaGenerator
    )
    always_fields = {
        "dataset": dataset,
        "source_value": source_value.value_string,
        "source_mime_type": source_value.mime_type,
        "source_label": source_value.label,
        "source_url": source_value.url,
        "source_value": source_value.value_string,
        "extraction_schema": schema,
        "extraction_checksum": extraction_config.checksum,
        "data_model": type(orig_extraction_data),
        "crawler_version": crawler_version,
        "orig_extraction_data": orig_extraction_data,
        "last_seen_version": context.version.id,
        "modified_at": datetime.now(timezone.utc),
        "modified_by": "zavod",
    }

    if review is None:
        # First insert
        context.log.debug("Requesting review", key=key_slug)
        review = Review(
            key=key_slug,
            extracted_data=orig_extraction_data,
            accepted=default_accepted,
            **always_fields,
        )  # type: ignore
        review.save(context.conn)
    # We're re-requesting review for an existing key for this dataset.
    else:
        if model_hash(orig_extraction_data) == model_hash(review.orig_extraction_data):
            # If the extraction result is the same as the original, we just store
            # a new version of the review with latest source data but keep the
            # acceptance status and any edits in extracted_data.
            accepted = review.accepted
            extracted_data = review.extracted_data
        else:
            accepted = default_accepted
            extracted_data = orig_extraction_data

        context.log.debug("Re-requesting review", key=key_slug)
        review.extracted_data = extracted_data
        review.accepted = accepted
        for key, value in always_fields.items():
            setattr(review, key, value)
        review.save(context.conn)
    return review


def should_extract(
    review: Optional[Review[ModelType]],
    source_value: SourceValue,
    extraction_config: ExtractionConfig[ModelType],
) -> bool:
    if review is None:
        return True

    if not review.accepted and review.extraction_checksum != extraction_config.checksum:
        return True

    if not source_value.matches(review):
        return True

    return False


def observe_source_value(
    context: Context,
    source_value: SourceValue,
    extraction_config: ExtractionConfig[ModelType],
) -> SourceObservation[ModelType]:
    """
    Get a review for a given key if it exists and update its last_seen_version to the current crawl version.

    Determine whether extraction should be performed for the given value and extraction config.

    Args:
        context: The runner context with dataset metadata.
        source_value: The source value for the extracted data.
        data_model: The pydantic data model class used to extract the data.
    """
    key_slug = review_key(source_value.key_parts)
    assert key_slug is not None
    review = Review[ModelType].by_key(
        context.conn, extraction_config.data_model, context.dataset.name, key_slug
    )

    if review is None:
        context.log.debug("Review not found", key=key_slug)
    else:
        context.log.debug("Review found, updating last_seen_version", key=key_slug)
        review.update_version(context.conn, context.version.id)

    return SourceObservation(
        review=review,
        should_extract=should_extract(review, source_value, extraction_config),
    )


def assert_all_accepted(context: Context, *, raise_on_unaccepted: bool = True) -> None:
    """
    Raise an exception or warning with the number of unaccepted items if any extraction
    entries for the current dataset and version are not accepted yet.

    Args:
        context: The runner context with dataset metadata.
        raise_on_unaccepted: Whether to raise an exception if there are unaccepted items.
            If False, a warning will be logged instead.
    """
    # Make sure everything is saved to the database in case we raise:
    context.flush()

    count = Review.count_unaccepted(
        context.conn, context.dataset.name, context.version.id
    )
    if count > 0:
        message = (
            f"There are {count} unaccepted items for dataset "
            f"{context.dataset.name} and version {context.version.id}"
        )
        if raise_on_unaccepted:
            raise Exception(message)
        else:
            context.log.warning(message)


def sort_arrays_in_value(value: JsonValue) -> JsonValue:
    """
    Recursively sort and serialize arrays within a json-serializable value.
    """
    if isinstance(value, list):
        return sorted(
            [sort_arrays_in_value(item) for item in value],
            key=lambda x: orjson.dumps(x, option=orjson.OPT_SORT_KEYS),
        )
    elif isinstance(value, dict):
        return {k: sort_arrays_in_value(v) for k, v in value.items()}
    return value


def model_hash(data: BaseModel) -> str:
    """
    Generate a consistent SHA-1 hash of orjson-serializable data.
    Arrays within the data are sorted to ensure the same hash regardless of order.
    """
    raw_data_dump = data.model_dump()
    # Sort arrays recursively to ensure consistent ordering
    sorted_data = sort_arrays_in_value(raw_data_dump)
    # Sort dictionary keys and convert to orjson
    raw_data_json = orjson.dumps(sorted_data, option=orjson.OPT_SORT_KEYS)
    return text_hash(raw_data_json.decode("utf-8"))
