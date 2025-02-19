import orjson
from pathlib import Path
from datetime import datetime
from functools import cached_property
from typing import Any, Optional, Union, Dict, List
from requests import Response
from prefixdate import DatePrefix
from lxml import html, etree
from datapatch import LookupException, Result, Lookup
from followthemoney.schema import Schema
from followthemoney.util import make_entity_id
from nomenklatura.versions import Version
from nomenklatura.cache import Cache
from nomenklatura.util import PathLike
from rigour.urls import build_url, ParamsType
from structlog.contextvars import clear_contextvars, bind_contextvars

from zavod import settings
from zavod.audit import inspect
from zavod.meta import Dataset, DataResource
from zavod.entity import Entity
from zavod.archive import dataset_resource_path, dataset_data_path
from zavod.runtime.versions import get_latest
from zavod.runtime.stats import ContextStats
from zavod.runtime.sink import DatasetSink
from zavod.runtime.issues import DatasetIssues
from zavod.runtime.resources import DatasetResources
from zavod.runtime.timestamps import TimeStampIndex
from zavod.runtime.cache import get_cache
from zavod.runtime.versions import make_version
from zavod.runtime.http_ import fetch_file, make_session, request_hash
from zavod.runtime.http_ import _Auth, _Headers, _Body
from zavod.logs import get_logger
from zavod.util import join_slug, prefixed_hash_id


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
        self.http = make_session(dataset.http)
        self._cache: Optional[Cache] = None
        self._timestamps: Optional[TimeStampIndex] = None

        self._data_time: datetime = settings.RUN_TIME
        # If the dataset has a fixed end time which is in the past,
        # use that as the data time:
        if dataset.coverage is not None and dataset.coverage.end is not None:
            if dataset.coverage.end < settings.RUN_TIME_ISO:
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
    def version(self) -> Version:
        """The current version of the dataset."""
        return get_latest(self.dataset.name, backfill=False) or settings.RUN_VERSION

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
        make_version(self.dataset, settings.RUN_VERSION, overwrite=clear)
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
        method: str = "GET",
        data: _Body = None,
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
            method=method,
            data=data,
        )

    def fetch_response(
        self,
        url: str,
        headers: _Headers = None,
        auth: _Auth = None,
        method: str = "GET",
        data: _Body = None,
    ) -> Response:
        """Execute an HTTP request using the contexts' session.

        Args:
            url: The URL to be fetched.
            headers: HTTP request headers to be included.
            auth: HTTP basic authorization username and password to be included.
            method: The HTTP method to use for the request.
            data: The data to be sent in the request body.
        Returns:
            A response object.
        """
        self.log.debug(f"HTTP {method}", url=url)
        timeout = (settings.HTTP_TIMEOUT, settings.HTTP_TIMEOUT)

        kwargs: Dict[str, Any] = {
            "headers": headers,
            "auth": auth,
            "timeout": timeout,
            "data": data,
        }

        # This mimics the allow_redirects login found in requests.sessions
        if method in ["GET", "OPTIONS"]:
            kwargs["allow_redirects"] = True
        elif method == "HEAD":
            kwargs["allow_redirects"] = False
        elif method in ["POST", "PUT", "PATCH", "DELETE"]:
            # Deliberately noop for the sake of explicitness
            pass
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response = self.http.request(
            method,
            url,
            **kwargs,
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
        method: str = "GET",
        data: _Body = None,
    ) -> Optional[str]:
        """Execute an HTTP request using the contexts' session and return
        the decoded response body. If a `cache_days` argument is provided, a
        cache will be used for the given number of days.

        Args:
            url: The URL to be fetched.
            params: URL query parameters to be included in the URL.
            headers: HTTP request headers to be included.
            auth: HTTP basic authorization username and password to be included.
            cache_days: Number of days to retain cached responses for. `None` to disable.
            method: The HTTP method to use for the request.
            data: The data to be sent in the request body.

        Returns:
            The decoded response body as a string.
        """
        url = build_url(url, params)

        if cache_days is not None:
            fingerprint = request_hash(url, auth=auth, method=method, data=data)
            text = None

            if method == "GET":
                # keeping the old caching keys that was GET requests only
                text = self.cache.get(url, max_age=cache_days)

            if text is None:
                # if the old cache is empty, try to get the cache by fingerprint
                text = self.cache.get(fingerprint, max_age=cache_days)

            if text is not None:
                self.log.debug("HTTP cache hit", url=url, fingerprint=fingerprint)
                return text

        response = self.fetch_response(
            url, headers=headers, auth=auth, method=method, data=data
        )
        text = response.text
        if text is None:
            return None

        if cache_days is not None:
            self.cache.set(fingerprint, text)
        return text

    def fetch_json(
        self,
        url: str,
        params: ParamsType = None,
        headers: _Headers = None,
        auth: _Auth = None,
        cache_days: Optional[int] = None,
        method: str = "GET",
        data: _Body = None,
    ) -> Any:
        """Execute an HTTP request using the contexts' session and return
        a JSON-decoded object based on the response. If a `cache_days` argument
        is provided, a cache will be used for the given number of days.

        Args:
            url: The URL to be fetched.
            params: URL query parameters to be included in the URL.
            headers: HTTP request headers to be included.
            auth: HTTP basic authorization username and password to be included.
            cache_days: Number of days to retain cached responses for.
            method: The HTTP method to use for the request.

        Returns:
            The decoded response body as a JSON-decoded object.
        """
        text = self.fetch_text(
            url,
            params=params,
            headers=headers,
            auth=auth,
            cache_days=cache_days,
            method=method,
            data=data,
        )

        if text is not None and len(text):
            try:
                return orjson.loads(text)
            except Exception:
                cache_url = build_url(url, params)
                fingerprint = request_hash(
                    cache_url, auth=auth, method=method, data=data
                )
                self.clear_url(fingerprint)
                raise

    def fetch_html(
        self,
        url: str,
        params: ParamsType = None,
        headers: _Headers = None,
        auth: _Auth = None,
        cache_days: Optional[int] = None,
        method: str = "GET",
        data: _Body = None,
    ) -> etree._Element:
        """Execute an HTTP request using the contexts' session and return
        an HTML DOM object based on the response. If a `cache_days` argument
        is provided, a cache will be used for the given number of days.

        Args:
            url: The URL to be fetched.
            params: URL query parameters to be included in the URL.
            headers: HTTP request headers to be included.
            auth: HTTP basic authorization username and password to be included.
            cache_days: Number of days to retain cached responses for.
            method: The HTTP method to use for the request.
            data: The data to be sent in the request body.
        Returns:
            An lxml-based DOM of the web page that has been returned.
        """
        text = self.fetch_text(
            url,
            params=params,
            headers=headers,
            auth=auth,
            cache_days=cache_days,
            method=method,
            data=data,
        )
        if text is not None and len(text):
            try:
                return html.fromstring(text)
            except Exception:
                cache_url = build_url(url, params)
                fingerprint = request_hash(
                    cache_url, auth=auth, method=method, data=data
                )
                self.clear_url(fingerprint)
                raise
        raise ValueError("Invalid HTML document: %s" % url)

    def clear_url(self, fingerprint: str) -> None:
        """
        Remove a given URL from the cache using request fingerprint
        Args:
            fingerprint: The unique fingerprint of the request.
        Returns:
            None
        """
        self.cache.delete(fingerprint)

    def parse_resource_xml(self, name: PathLike) -> etree._ElementTree:
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
        self,
        *parts: Optional[str],
        prefix: Optional[str] = None,
        hash_prefix: Optional[str] = None,
    ) -> Optional[str]:
        """Make a hash-based entity ID from a list of strings, prefixed with the
        dataset prefix.

        Args:
            prefix: Use this prefix in the slug, but not the hash.
            hash_prefix: Use this prefix in the hash, but not the slug.
        """
        hash_prefix = hash_prefix or self.dataset.name
        hashed = make_entity_id(*parts, key_prefix=hash_prefix)
        if hashed is None:
            return None
        prefix = self.dataset.prefix if prefix is None else prefix
        return prefixed_hash_id(prefix, hashed)

    def lookup_value(
        self, lookup: str, value: Optional[str], default: Optional[str] = None
    ) -> Optional[str]:
        """Invoke a datapatch lookup defined in the dataset metadata.

        Args:
            lookup: The name of the lookup. The key under the dataset lookups property.
            value: The data value to look up.
            default: The default value to use if the lookup doesn't match the value.
        """
        try:
            lookup_obj = self.get_lookup(lookup)
            return lookup_obj.get_value(value, default=default)
        except LookupException:
            return default

    def get_lookup(self, lookup: str) -> Lookup:
        return self.dataset.lookups[lookup]

    def lookup(self, lookup: str, value: Optional[str]) -> Optional[Result]:
        return self.get_lookup(lookup).match(value)

    def debug_lookups(self) -> None:
        """Output a list of unused lookup options."""
        for name, lookup in self.dataset.lookups.items():
            for option in lookup.options:
                if option.ref_count > 0:
                    continue
                self.log.warn(
                    "Unused lookup option",
                    lookup=name,
                    option=repr(option),
                    clauses=option.clauses,
                )
            # print(lookup.unmatched_yaml())

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
            self.log.error("Entity has no properties", entity=entity)
            return
        self.stats.entities += 1
        if self.stats.entities % 10000 == 0:
            self.log.info(
                "Emitted %s entities" % self.stats.entities,
                statements=self.stats.statements,
            )
        stamps = {} if self.dry_run else self.timestamps.get(entity.id)
        for stmt in entity.statements:
            if stmt.id is None:
                self.log.warn("Statement has no ID", stmt=stmt.to_dict())
                continue
            if stmt.lang is None:
                stmt.lang = self.lang
            stmt.dataset = self.dataset.name
            stmt.entity_id = entity.id
            stmt.external = external
            stmt.schema = entity.schema.name
            stmt.first_seen = stamps.get(stmt.id, self.data_time_iso)
            if stmt.first_seen != self.data_time_iso:
                self.stats.changed += 1
            stmt.last_seen = self.data_time_iso
            if not self.dry_run:
                self.sink.emit(stmt)
            self.stats.statements += 1

    def __hash__(self) -> int:
        return hash(self.dataset.name)

    def __repr__(self) -> str:
        return f"<Context({self.dataset.name})>"
