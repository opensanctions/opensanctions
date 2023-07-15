import json
import hashlib
import mimetypes
from pathlib import Path
from functools import cached_property
from typing import Dict, Optional, Any, List
from lxml import etree, html
from sqlalchemy.exc import OperationalError
from requests.exceptions import RequestException
from datapatch import LookupException, Result, Lookup
from structlog.contextvars import clear_contextvars, bind_contextvars
from nomenklatura.cache import Cache
from nomenklatura.util import normalize_url
from nomenklatura.resolver import Resolver
from nomenklatura.statement import Statement
from nomenklatura.statement.serialize import PackStatementWriter

from zavod import settings as zavod_settings
from zavod.context import Context as ZavodContext
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.runner.util import load_method
from zavod.archive import dataset_path, STATEMENTS_RESOURCE
from opensanctions import settings
from opensanctions.core.catalog import get_catalog
from opensanctions.core.db import engine, engine_tx, metadata
from opensanctions.core.issues import IssueWriter
from opensanctions.core.timestamps import TimeStampIndex
from opensanctions.core.resolver import get_resolver
from opensanctions.core.resources import save_resource, clear_resources
from opensanctions.core.statements import cleanup_dataset, clear_statements
from opensanctions.core.statements import save_statements, lock_dataset


class Context(ZavodContext):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    SOURCE_TITLE = "Source data"
    SOURCE_CATEGORY = "source"
    BATCH_SIZE = 5000

    def __init__(self, dataset: Dataset, dry_run: bool = False):
        super().__init__(dataset, Entity)
        self.cache = Cache(engine, metadata, dataset, create=True)
        self.issues = IssueWriter(dataset)
        self.dry_run = dry_run
        self._stmt_writer: Optional[PackStatementWriter] = None
        self._statements: Dict[str, Statement] = {}
        self._entity_count = 0
        self._statement_count = 0

    @property
    def source(self) -> Dataset:
        if self.dataset.data is not None:
            return self.dataset
        raise RuntimeError("Dataset is not a source: %s" % self.dataset.name)

    @property
    def resolver(self) -> Resolver:
        return get_resolver()

    @cached_property
    def lang(self) -> Optional[str]:
        if self.dataset.data is not None:
            return self.dataset.data.lang
        return None

    @cached_property
    def timestamps(self) -> "TimeStampIndex":
        return TimeStampIndex.build(self.dataset)

    def bind(self) -> None:
        bind_contextvars(
            dataset=self.dataset.name,
            _context=self,
        )

    def close(self) -> None:
        """Flush and tear down the context."""
        self.cache.close()
        self.issues.close()
        if self._stmt_writer is not None:
            self._stmt_writer.close()
        super().close()
        clear_contextvars()

    def fetch_response(self, url, headers=None, auth=None):
        self.log.debug("HTTP GET", url=url)
        timeout = (zavod_settings.HTTP_TIMEOUT, zavod_settings.HTTP_TIMEOUT)
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
        url,
        params=None,
        headers=None,
        auth=None,
        cache_days=None,
    ):
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

    def fetch_json(self, *args, **kwargs):
        """Fetch the given URL (GET) and decode it as a JSON object."""
        text = self.fetch_text(*args, **kwargs)
        if text is not None and len(text):
            return json.loads(text)

    def fetch_html(self, *args, **kwargs):
        text = self.fetch_text(*args, **kwargs)
        if text is not None and len(text):
            return html.fromstring(text)

    def parse_resource_xml(self, name):
        """Parse a file in the resource folder into an XML tree."""
        file_path = self.get_resource_path(name)
        with open(file_path, "rb") as fh:
            return etree.parse(fh)

    def export_resource(
        self,
        path: Path,
        mime_type: Optional[str] = None,
        title: Optional[str] = None,
        category: str = SOURCE_CATEGORY,
    ) -> None:
        """Register a file as a documented file exported by the dataset."""
        if mime_type is None:
            mime_type, _ = mimetypes.guess(path)

        digest = hashlib.sha1()
        size = 0
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(65536)
                if not chunk:
                    break
                size += len(chunk)
                digest.update(chunk)
        if size == 0:
            self.log.warning("Resource is empty", path=path)
        checksum = digest.hexdigest()
        dataset_path_ = dataset_path(self.dataset.name)
        name = path.relative_to(dataset_path_).as_posix()
        with engine_tx() as conn:
            return save_resource(
                conn,
                name,
                self.dataset,
                checksum,
                mime_type,
                category,
                size,
                title,
            )

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

    def audit_data(
        self, data: Dict[Optional[str], Any], ignore: List[str] = []
    ) -> None:
        """Print a row if any of the fields not ignored are still unused."""
        cleaned = {}
        for key, value in data.items():
            if key in ignore:
                continue
            if value is None or value == "":
                continue
            cleaned[key] = value
        if len(cleaned):
            self.log.warn("Unexpected data found", data=cleaned)

    def flush(self) -> None:
        """Emitted entities are de-constructed into statements for the database
        to store. These are inserted in batches - so the statement cache on the
        context is flushed to the store. All statements that are not flushed
        when a crawl is aborted are not persisted to the database."""
        statements = list(self._statements.values())
        if self.dry_run:
            self.log.info(
                "Dry run: discarding %d statements..." % len(statements),
                entities=self._entity_count,
                total=self._statement_count,
            )
            self._statements = {}
            return

        if len(statements):
            self._statement_count += len(statements)
            self.log.info(
                "Storing %d statements..." % len(statements),
                entities=self._entity_count,
                total=self._statement_count,
            )
        with engine_tx() as conn:
            lock_dataset(conn, self.dataset)
            for i in range(0, len(statements), self.BATCH_SIZE):
                batch = statements[i : i + self.BATCH_SIZE]
                save_statements(conn, batch)
        self._statements = {}

    def emit(
        self,
        entity: Entity,
        target: Optional[bool] = None,
        external: bool = False,
    ):
        """Send an FtM entity to the store."""
        if entity.id is None:
            raise ValueError("Entity has no ID: %r", entity)
        canonical_id = self.resolver.get_canonical(entity.id)
        for stmt in entity.statements:
            assert stmt.dataset == self.dataset.name, (
                stmt.prop,
                entity.dataset.name,
                stmt.dataset,
                self.dataset.name,
            )
            assert stmt.entity_id == entity.id
            stmt.canonical_id = canonical_id
            stmt.schema = entity.schema.name
            stmt.last_seen = settings.RUN_TIME_ISO
            if not self.dry_run:
                stmt.first_seen = self.timestamps.get(stmt.id)
            stmt.target = target or False
            stmt.external = external
            if stmt.lang is None and self.lang is not None:
                stmt.lang = self.lang
            # if not self.dry_run:
            if self._stmt_writer is None:
                fh = open(self.get_resource_path(STATEMENTS_RESOURCE), "wb")
                self._stmt_writer = PackStatementWriter(fh)
            self._stmt_writer.write(stmt)
            self._statements[stmt.id] = stmt
        self.log.debug("Emitted", entity=entity.id, schema=entity.schema.name)
        self._entity_count += 1
        if len(self._statements) >= (self.BATCH_SIZE * 10):
            self.flush()

    def crawl(self) -> bool:
        """Run the crawler."""
        self.bind()
        if self.source.disabled:
            self.log.info("Source is disabled")
            return True
        self.issues.clear()
        with engine_tx() as conn:
            clear_resources(conn, self.dataset, category=self.SOURCE_CATEGORY)
        self.log.info("Begin crawl", run_time=settings.RUN_TIME_ISO)
        self._entity_count = 0
        self._statement_count = 0
        try:
            # Run the dataset:
            method = load_method(self.dataset)
            method(self)
            self.flush()
            if self._entity_count == 0:
                self.log.warn(
                    "Crawler did not emit entities",
                    statements=self._statement_count,
                )
            else:
                if not self.dry_run:
                    with engine_tx() as conn:
                        cleanup_dataset(conn, self.dataset)
            self.log.info("Crawl completed", entities=self._entity_count)
            return True
        except KeyboardInterrupt:
            self.log.warning("Aborted by user (SIGINT)")
            return False
        except LookupException as lexc:
            self.log.error(lexc.message, lookup=lexc.lookup.name, value=lexc.value)
            return False
        except OperationalError as oexc:
            self.log.error("Database error: %r" % oexc)
            return False
        except RequestException as rexc:
            resp = repr(rexc.response)
            self.log.error(str(rexc), url=rexc.request.url, response=resp)
            return False
        except Exception as exc:
            self.log.exception("Crawl failed", error=str(exc))
            raise
        finally:
            self.close()

    def clear(self, data: bool = True) -> None:
        """Delete all recorded data for a given dataset."""
        with engine_tx() as conn:
            self.issues.clear()
            clear_resources(conn, self.dataset)
            if data:
                self.cache.clear()
                path = self.get_resource_path(STATEMENTS_RESOURCE)
                if path.exists():
                    path.unlink()
                clear_statements(conn, self.dataset)
