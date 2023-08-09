import json
from lxml import html, etree
from pathlib import Path
from datetime import datetime
from requests import Response
from prefixdate import DatePrefix
from functools import cached_property
from typing import Any, Optional, Union, Dict, List, Tuple, Mapping
from datapatch import LookupException, Result, Lookup
from followthemoney.schema import Schema
from followthemoney.util import make_entity_id
from nomenklatura.cache import Cache
from nomenklatura.util import normalize_url, ParamsType, PathLike
from structlog.contextvars import clear_contextvars, bind_contextvars

from zavod import settings
from zavod.audit import inspect
from zavod.meta import Dataset, DataResource, get_catalog
from zavod.entity import Entity
from zavod.archive import dataset_resource_path, dataset_data_path
from zavod.runtime.stats import ContextStats
from zavod.runtime.sink import DatasetSink
from zavod.runtime.issues import DatasetIssues
from zavod.runtime.resources import DatasetResources
from zavod.runtime.timestamps import TimeStampIndex
from zavod.runtime.cache import get_cache
from zavod.http import fetch_file, make_session
from zavod.logs import get_logger
from zavod.util import join_slug, ElementOrTree

_Auth = Optional[Tuple[str, str]]
_Headers = Optional[Mapping[str, str]]


class Context:
    """The context is a utility object that is passed as an argument into crawlers
    and other runners.

    It supports creating and emitting (storing) entities, accessing metadata and
    logging errors and warnings. It also has functions for fetching data from the
    web and storing it in the dataset's data folder.
    """

    SOURCE_TITLE = "Source data"

    def __init__(self, dataset: Dataset, dry_run: bool = False):
        self.dataset = dataset
        self.dry_run = dry_run
        self.stats = ContextStats()
        self.sink = DatasetSink(dataset)
        self.issues = DatasetIssues(dataset)
        self.resources = DatasetResources(dataset)
        self.log = get_logger(dataset.name)
        self.http = make_session()
        self._cache: Optional[Cache] = None
        self._timestamps: Optional[TimeStampIndex] = None

        self._data_time: datetime = settings.RUN_TIME
        # If the dataset has a fixed end time, use that as the data time:
        if dataset.coverage is not None and dataset.coverage.end is not None:
            prefix = DatePrefix(dataset.coverage.end)
            self._data_time = prefix.dt or self._data_time

        self.lang: Optional[str] = None
        """Default language for statements emitted from this dataset"""
        if dataset.data is not None:
            self.lang = dataset.data.lang

    @property
    def cache(self) -> Cache:
        """A cache object for storing HTTP responses and other data."""
        if self._cache is None:
            self._cache = get_cache(self.dataset)
        return self._cache

    @property
    def timestamps(self) -> TimeStampIndex:
        """An index of the first_seen time of every statement previous emitted by
        the dataset. This is used to determine if a statement is new or not."""
        if self._timestamps is None:
            self._timestamps = TimeStampIndex.build(self.dataset)
        return self._timestamps

    @property
    def data_url(self) -> str:
        """The URL of the source data for the dataset."""
        if self.dataset.data is None or self.dataset.data.url is None:
            raise ValueError("Dataset has no data URL: %r" % self.dataset)
        return self.dataset.data.url

    @property
    def data_time(self) -> datetime:
        """The data provenance time to be used for the emitted statements. This is
        used to set the first_seen and last_seen properties of statements to a time
        that may be different than the real run time of the crawler, e.g. when a
        coverage end is defined, or the data source itself states an update time.

        Returns:
            The time to be used for the emitted statements."""
        return self._data_time

    @data_time.setter
    def data_time(self, value: datetime) -> None:
        """Modify the data time."""
        self._data_time = value
        del self.data_time_iso

    @cached_property
    def data_time_iso(self) -> str:
        """String representation of `data_time` in ISO format."""
        return self.data_time.isoformat(sep="T", timespec="seconds")

    def begin(self, clear: bool = False) -> None:
        """Prepare the context for running the exporter.

        Args:
            clear: Remove the existing resources and issues from the dataset.
        """
        bind_contextvars(
            dataset=self.dataset.name,
            context=self,
        )
        if clear and not self.dry_run:
            self.resources.clear()
            self.issues.clear()
        self.stats.reset()

    def close(self) -> None:
        """Flush and tear down the context."""
        self.http.close()
        if self._cache is not None:
            self._cache.close()
        if self._timestamps is not None:
            self._timestamps.close()
        self.sink.close()
        clear_contextvars()
        self.issues.close()
        if not self.dry_run:
            self.issues.export()

    def get_resource_path(self, name: PathLike) -> Path:
        """Get the path to a file in the dataset data folder.

        Args:
            name: The name of the file, relative to the dataset data folder.

        Returns:
            The full path to the file."""
        return dataset_resource_path(self.dataset.name, str(name))

    def export_resource(
        self, path: Path, mime_type: Optional[str] = None, title: Optional[str] = None
    ) -> DataResource:
        """Register a file as a data resource exported by the dataset.

        Args:
            path: The file path of the exported resource
            mime_type: MIME type of the resource, will be guessed otherwise
            title: A human-readable description.

        Returns:
            The generated resource object which has been saved.
        """
        resource = DataResource.from_path(
            self.dataset, path, mime_type=mime_type, title=title
        )
        if not self.dry_run:
            self.resources.save(resource)
        return resource

    def fetch_resource(
        self,
        name: str,
        url: str,
        auth: Optional[Any] = None,
        headers: Optional[Any] = None,
    ) -> Path:
        """Fetch a URL into a file located in the current run folder,
        if it does not exist."""
        return fetch_file(
            self.http,
            url,
            name,
            data_path=dataset_data_path(self.dataset.name),
            auth=auth,
            headers=headers,
        )

    def fetch_response(
        self, url: str, headers: _Headers = None, auth: _Auth = None
    ) -> Response:
        """Execute an HTTP GET request using the contexts' session.

        Args:
            url: The URL to be fetched.
            headers: HTTP request headers to be included.
            auth: HTTP basic authorization username and password to be included.

        Returns:
            A response object.
        """
        self.log.debug("HTTP GET", url=url)
        timeout = (settings.HTTP_TIMEOUT, settings.HTTP_TIMEOUT)
        response = self.http.get(
            url,
            headers=headers,
            auth=auth,
            timeout=timeout,
            allow_redirects=True,
        )
        response.raise_for_status()
        return response

    def fetch_text(
        self,
        url: str,
        params: ParamsType = None,
        headers: _Headers = None,
        auth: _Auth = None,
        cache_days: Optional[int] = None,
    ) -> str:
        """Execute an HTTP GET request using the contexts' session and return
        the decoded response body. If a `cache_days` argument is provided, a
        cache will be used for the given number of days.

        Args:
            url: The URL to be fetched.
            params: URL query parameters to be included in the URL.
            headers: HTTP request headers to be included.
            auth: HTTP basic authorization username and password to be included.
            cache_days: Number of days to retain cached responses for.

        Returns:
            The decoded response body as a string.
        """
        url = normalize_url(url, params)
        if cache_days is not None:
            text = self.cache.get(url, max_age=cache_days)
            if text is not None:
                self.log.debug("HTTP cache hit", url=url)
                return text

        response = self.fetch_response(url, headers=headers, auth=auth)
        text = response.text
        if text is None:
            return None

        if cache_days is not None:
            self.cache.set(url, text)
        return text

    def fetch_json(
        self,
        url: str,
        params: ParamsType = None,
        headers: _Headers = None,
        auth: _Auth = None,
        cache_days: Optional[int] = None,
    ) -> Any:
        """Execute an HTTP GET request using the contexts' session and return
        a JSON-decoded object based on the response. If a `cache_days` argument
        is provided, a cache will be used for the given number of days.

        Args:
            url: The URL to be fetched.
            params: URL query parameters to be included in the URL.
            headers: HTTP request headers to be included.
            auth: HTTP basic authorization username and password to be included.
            cache_days: Number of days to retain cached responses for.

        Returns:
            The decoded response body as a JSON-decoded object.
        """
        text = self.fetch_text(
            url,
            params=params,
            headers=headers,
            auth=auth,
            cache_days=cache_days,
        )
        if text is not None and len(text):
            return json.loads(text)

    def fetch_html(
        self,
        url: str,
        params: ParamsType = None,
        headers: _Headers = None,
        auth: _Auth = None,
        cache_days: Optional[int] = None,
    ) -> ElementOrTree:
        """Execute an HTTP GET request using the contexts' session and return
        an HTML DOM object based on the response. If a `cache_days` argument
        is provided, a cache will be used for the given number of days.

        Args:
            url: The URL to be fetched.
            params: URL query parameters to be included in the URL.
            headers: HTTP request headers to be included.
            auth: HTTP basic authorization username and password to be included.
            cache_days: Number of days to retain cached responses for.

        Returns:
            An lxml-based DOM of the web page that has been returned.
        """
        text = self.fetch_text(
            url,
            params=params,
            headers=headers,
            auth=auth,
            cache_days=cache_days,
        )
        if text is not None and len(text):
            return html.fromstring(text)
        raise ValueError("Invalid HTML document: %s" % url)

    def parse_resource_xml(self, name: PathLike) -> ElementOrTree:
        """Parse a file in the resource folder into an XML tree.

        Args:
            name: The resource name or relative file path.

        Returns:
            An lxml element tree of the parsed XML.
        """
        file_path = self.get_resource_path(name)
        with open(file_path, "rb") as fh:
            return etree.parse(fh)

    def make(self, schema: Union[str, Schema]) -> Entity:
        """Make a new entity with some dataset context set.

        Args:
            schema: The entity's type name

        Returns:
            A newly created entity object of the given type, with no ID.
        """
        return Entity(self.dataset, {"schema": schema})

    def make_slug(
        self, *parts: Optional[str], strict: bool = True, prefix: Optional[str] = None
    ) -> Optional[str]:
        """Make a slug-based entity ID from a list of strings, using the
        dataset prefix."""
        prefix = self.dataset.prefix if prefix is None else prefix
        return join_slug(*parts, prefix=prefix, strict=strict)

    def make_id(
        self, *parts: Optional[str], prefix: Optional[str] = None
    ) -> Optional[str]:
        """Make a hash-based entity ID from a list of strings, prefixed with the
        dataset prefix."""
        hashed = make_entity_id(*parts, key_prefix=self.dataset.name)
        if hashed is None:
            return None
        return self.make_slug(hashed, prefix=prefix, strict=True)

    def lookup_value(
        self,
        lookup: str,
        value: Optional[str],
        default: Optional[str] = None,
        dataset: Optional[str] = None,
    ) -> Optional[str]:
        try:
            lookup_obj = self.get_lookup(lookup, dataset=dataset)
            return lookup_obj.get_value(value, default=default)
        except LookupException:
            return default

    def get_lookup(self, lookup: str, dataset: Optional[str] = None) -> Lookup:
        ds = get_catalog().require(dataset) if dataset is not None else self.dataset
        return ds.lookups[lookup]

    def lookup(
        self, lookup: str, value: Optional[str], dataset: Optional[str] = None
    ) -> Optional[Result]:
        return self.get_lookup(lookup, dataset=dataset).match(value)

    def inspect(self, obj: Any) -> None:
        """Display an object in a form suitable for inspection.

        Args:
            obj: The object to be logged in pretty print.
        """
        text = inspect(obj)
        if text is not None:
            self.log.info(text)

    def audit_data(self, data: Dict[Any, Any], ignore: List[Any] = []) -> None:
        """Print the formatted data object if it contains any fields not explicitly
        excluded by the ignore list. This is used to warn about unexpected data in
        the source by removing the fields one by one and then inspecting the rest.

        Args:
            data: A mapping which is to be checked.
            ignore: List of string keys to be skipped when checking the mapping
        """
        cleaned = {}
        for key, value in data.items():
            if key in ignore:
                continue
            if value is None or value == "":
                continue
            cleaned[key] = value
        if len(cleaned):
            self.log.warn("Unexpected data found", data=cleaned)

    def emit(
        self, entity: Entity, target: bool = False, external: bool = False
    ) -> None:
        """Send an entity from the crawling/runner process to be stored.

        Args:
            entity: The entity to be stored.
            target: Whether the entity is a target of the dataset.
            external: Whether the entity is an enrichment candidate or already
                part of the dataset.
        """
        if entity.id is None:
            raise ValueError("Entity has no ID: %r", entity)
        if len(entity.properties) == 0:
            raise ValueError("Entity has no properties: %r", entity)
        self.stats.entities += 1
        if target:
            self.stats.targets += 1
        if self.stats.entities % 10000 == 0:
            self.log.info(
                "Emitted %s entities" % self.stats.entities,
                targets=self.stats.targets,
                statements=self.stats.statements,
            )
        for stmt in entity.statements:
            if stmt.lang is None:
                stmt.lang = self.lang
            stmt.dataset = self.dataset.name
            stmt.entity_id = entity.id
            stmt.external = external
            stmt.target = target
            stmt.schema = entity.schema.name
            stmt.first_seen = self.data_time_iso
            stmt.last_seen = self.data_time_iso
            if not self.dry_run:
                stmt.first_seen = self.timestamps.get(stmt.id, self.data_time_iso)
                self.sink.emit(stmt)
            self.stats.statements += 1

    def __hash__(self) -> int:
        return hash(self.dataset.name)

    def __repr__(self) -> str:
        return f"<Context({self.dataset.name})>"
