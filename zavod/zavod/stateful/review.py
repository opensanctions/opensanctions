from abc import ABC
from datetime import datetime, timezone
from hashlib import sha1
from logging import getLogger
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

import orjson
from lxml.html import HtmlElement, fromstring, tostring
from normality import slugify
from pydantic import BaseModel, JsonValue, PrivateAttr
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode
from pydantic_core import CoreSchema
from rigour.mime.types import HTML, PLAIN
from rigour.text import text_hash
from sqlalchemy import func, insert, not_, select, update
from sqlalchemy.engine import Connection
from sqlalchemy.sql import Select

from zavod import helpers as h
from zavod.context import Context
from zavod.stateful.model import review_table

log = getLogger(__name__)

ModelType = TypeVar("ModelType", bound=BaseModel)

MODIFIED_BY_CRAWLER = "zavod"


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
    """A slug derived from some information from the source that uniquely and
    consistently identifies the review within the dataset. For an enforcement action,
    that might be an action reference number or publication url (for lack of a consistent identifier).
    For a string of names that rarely changes, that string itself might work."""
    dataset: str
    extraction_schema: JsonValue
    source_value: str
    """The value of the original text, or a url to e.g. an archived image from
    which the data was extracted."""
    source_mime_type: str
    """The mime type of the source value. Useful to know how to display
    the source value to the reviewer."""
    source_label: str
    """Used to indicate the context of the source value to the reviewer,
    e.g. "Banking Organization field in CSV" or "Screenshot of PDF page"."""
    source_url: Optional[str] = None
    data_model: Type[ModelType]
    crawler_version: int
    _original_extraction: JsonValue = PrivateAttr()
    """Only to be edited by the crawler"""
    origin: Optional[str] = None
    _extracted_data: JsonValue = PrivateAttr()
    """Editable by the reviewer"""
    accepted: bool
    last_seen_version: str
    """The crawl version that last saw this review key"""
    modified_at: datetime
    modified_by: str
    deleted_at: Optional[datetime] = None

    # Lazily validating the pydantic model fields lets us populate the Review
    # and check self.crawler_version before validating the data, which is useful
    # for breaking changes.

    @property
    def original_extraction(self) -> ModelType:
        return self.data_model.model_validate(self._original_extraction)

    @original_extraction.setter
    def original_extraction(self, value: ModelType) -> None:
        self._original_extraction = value.model_dump()

    @property
    def extracted_data(self) -> ModelType:
        return self.data_model.model_validate(self._extracted_data)

    @extracted_data.setter
    def extracted_data(self, value: ModelType) -> None:
        self._extracted_data = value.model_dump()

    def __init__(self, **data):  # type: ignore
        super().__init__(**data)
        if "_original_extraction" in data:
            self._original_extraction = data.pop("_original_extraction")
        else:
            self._original_extraction = data.pop("original_extraction").model_dump()
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
        review_data["_original_extraction"] = review_data.pop("original_extraction")
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

    def save(self, conn: Connection, new_revision: bool) -> None:
        values = {
            "key": self.key,
            "dataset": self.dataset,
            "extraction_schema": self.extraction_schema,
            "source_value": self.source_value,
            "source_mime_type": self.source_mime_type,
            "source_label": self.source_label,
            "source_url": self.source_url,
            "crawler_version": self.crawler_version,
            "original_extraction": self._original_extraction,
            "origin": self.origin,
            "extracted_data": self._extracted_data,
            "accepted": self.accepted,
            "last_seen_version": self.last_seen_version,
            "modified_at": self.modified_at,
            "modified_by": self.modified_by,
        }
        if new_revision:
            if self.id:
                conn.execute(
                    update(review_table)
                    .where(review_table.c.id == self.id)
                    .values(deleted_at=datetime.now(timezone.utc))
                )
            ins = insert(review_table).values(**values)
            result = conn.execute(ins)
            assert result.inserted_primary_key is not None
            self.id = result.inserted_primary_key[0]
        else:
            assert self.id is not None
            upd = (
                update(review_table)
                .where(review_table.c.id == self.id)
                .values(**values)
            )
            result = conn.execute(upd)

    @classmethod
    def count_unaccepted(cls, conn: Connection, dataset: str, version_id: str) -> int:
        select_stmt = select(func.count(review_table.c.id)).where(
            review_table.c.dataset == dataset,
            review_table.c.last_seen_version == version_id,
            not_(review_table.c.accepted),
        )
        return conn.execute(select_stmt).scalar_one()


class SourceValue(ABC):
    """
    A container for a value that can be serialized for storage and compared in
    a way that detects changes that justify re-extraction and and potentially
    re-review.
    """

    key_parts: str | List[str]
    """The key parts will be slugified and shorted with a hash of all the
    parts if the slug would be too long."""
    mime_type: str
    label: str
    url: Optional[str]
    value_string: str

    def matches(self, review: Review[ModelType]) -> bool:
        raise NotImplementedError


class TextSourceValue(SourceValue):
    def __init__(
        self,
        key_parts: str | List[str],
        label: str,
        text: str,
        url: Optional[str] = None,
    ):
        """
        Args:
            key_parts: Information from the source that uniquely and
                consistently identifies the review within the dataset. For a string
                of names that rarely changes, that string itself might work.
        """
        self.key_parts = key_parts
        self.mime_type = PLAIN
        self.label = label
        self.url = url
        self.value_string = text

    def matches(self, review: Review[ModelType]) -> bool:
        """
        Performs the same normalisation as `review_key` so that we consider
        multiple source values normalising to the same key as a match.
        """
        assert review.source_mime_type == PLAIN, review.source_mime_type
        return slugify(self.value_string) == slugify(review.source_value)


class HtmlSourceValue(SourceValue):
    element: HtmlElement

    def __init__(
        self,
        key_parts: str | List[str],
        label: str,
        element: HtmlElement,
        url: str,
    ):
        """
        Sets `value_string` as the serialized HTML of the element.

        Args:
            key_parts: Information from the source that uniquely and
                consistently identifies the review within the dataset. For an
                enforcement action, that might be an action reference number or
                publication url (for lack of a consistent identifier).
        """
        self.key_parts = key_parts
        self.mime_type = HTML
        self.label = label
        self.url = url
        self.value_string = tostring(element, pretty_print=True, encoding="unicode")
        self.element = element

    def matches(self, review: Review[ModelType]) -> bool:
        assert review.source_mime_type == HTML, review.source_mime_type
        seen_element = fromstring(review.source_value)
        return h.html.element_text_hash(seen_element) == h.html.element_text_hash(
            self.element
        )


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


def review_extraction(
    context: Context,
    source_value: SourceValue,
    original_extraction: ModelType,
    origin: str,
    crawler_version: int = 1,
    default_accepted: bool = False,
) -> Review[ModelType]:
    """
    Ensures a Review exists for the given key to allow human review of automated
    data extraction from a source value.

    - If it's new, `extracted_data` will default to `original_extraction` and
      `accepted` to `default_accepted`.
    - `last_seen_version` is always updated to the current crawl version.
    - If it's not accepted yet, `original_extraction` and `extracted_data` will be updated.
    - If `source_value` AND the `original_extraction` have changed, they
      will be updated and acceptance status will be reset to `default_accepted`.
    - Updating `crawler_version` resets `extraction_data` and `acceptance` status
      as well as updating `original_extraction` and `extraction_schema`.

    Args:
        context: The runner context with dataset metadata.
        source_value: The source value for the extracted data.
        original_extraction: An instance of a pydantic model of data extracted
            from the source. Any reviewer changes to the data will be validated against
            this model. Initially `Review.extracted_data` will be set to this value.
            Reviewer edits will then be stored in `Review.extracted_data` with
            `Review.original_extraction` remaining as the original.
        origin: A short string indicating the origin of the extraction, e.g. the
            model name or "lookups" if it's backfilled from datapatches lookups.
        crawler_version: A version number a crawler can use as a checkpoint for changes
            requiring re-extraction and/or re-review.
            Useful e.g. when breaking model changes are made.
        default_accepted: Whether the data should be marked as accepted on creation or reset.
    """
    key_slug = review_key(source_value.key_parts)
    assert key_slug is not None

    data_model = type(original_extraction)
    schema = data_model.model_json_schema(schema_generator=SchemaGenerator)
    now = datetime.now(timezone.utc)

    review = Review.by_key(
        context.conn, data_model, dataset=context.dataset.name, key=key_slug
    )
    save_new_revision = False
    if review is None:
        context.log.debug("Creating new review", key=key_slug)
        review = Review(
            dataset=context.dataset.name,
            key=key_slug,
            source_mime_type=source_value.mime_type,
            source_label=source_value.label,
            source_url=source_value.url,
            accepted=default_accepted,
            extraction_schema=schema,
            source_value=source_value.value_string,
            data_model=data_model,
            original_extraction=original_extraction,
            origin=origin,
            extracted_data=original_extraction,
            crawler_version=crawler_version,
            last_seen_version=context.version.id,
            modified_at=now,
            modified_by=MODIFIED_BY_CRAWLER,
        )
        save_new_revision = True
    else:
        review.last_seen_version = context.version.id

        crawler_version_changed = review.crawler_version < crawler_version
        # Don't try to read (and thus validate) the extracted data if the crawler
        # version changed. We bump that when it's backward incompatible.
        if crawler_version_changed or (
            not source_value.matches(review)
            and (
                model_hash(original_extraction)
                != model_hash(review.original_extraction)
            )
        ):
            if crawler_version_changed:
                context.log.debug(
                    "Crawler version changed. Resetting review.",
                    key=key_slug,
                    old=review.crawler_version,
                    new=crawler_version,
                )
            else:
                context.log.debug(
                    "Source value changed. Resetting review.", key=key_slug
                )
            # If the crawler version changed, we want to update it.
            # This is useful for breaking changes in the extraction model.
            # If the source and also the automated extraction changed,
            # we also want to reset.
            review.crawler_version = crawler_version
            review.data_model = data_model
            review.extraction_schema = schema
            review.original_extraction = original_extraction
            review.origin = origin
            review.extracted_data = original_extraction
            review.accepted = default_accepted
            save_new_revision = True
        elif not review.accepted and (
            model_hash(original_extraction) != model_hash(review.original_extraction)
        ):
            context.log.debug("Extraction changed for unaccepted review.", key=key_slug)
            # If we haven't accepted this yet and the extraction changed, we want the
            # change regardless of whether the source changed since the prompt or the
            # model might be better.
            review.original_extraction = original_extraction
            # Resetting extracted_data to original_extraction here loses unaccepted edits
            # but prompt improvements happen more often than unaccepted edits.
            review.extracted_data = original_extraction
            # Saving a new revision every crawl for an unaccepted item makes a revision
            # for items where two items mapping to the same key, e.g.
            # "American Express Inc" and "American Express Inc." when we're likely not
            # a ton of reviewer work.
            # Once accepted, the one extraction won't be overwriting the other.
            save_new_revision = False

        if save_new_revision:
            review.modified_at = now
            review.modified_by = MODIFIED_BY_CRAWLER
    review.save(context.conn, new_revision=save_new_revision)
    return review


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
