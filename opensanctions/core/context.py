import json
from functools import cached_property
from typing import Dict, Optional
from lxml import etree, html
from sqlalchemy.exc import OperationalError
from requests.exceptions import RequestException
from datapatch import LookupException
from nomenklatura.util import normalize_url
from nomenklatura.resolver import Resolver
from nomenklatura.statement import Statement

from zavod import settings
from zavod.context import Context as ZavodContext
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.dedupe import get_resolver
from zavod.runtime.loader import load_entry_point
from zavod.runtime.timestamps import TimeStampIndex
from opensanctions.core.db import engine_tx
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
        super().__init__(dataset, dry_run=dry_run)
        self._statements: Dict[str, Statement] = {}

    @property
    def source(self) -> Dataset:
        if self.dataset.data is not None:
            return self.dataset
        raise RuntimeError("Dataset is not a source: %s" % self.dataset.name)

    @property
    def resolver(self) -> Resolver:
        return get_resolver()

    @cached_property
    def timestamps(self) -> "TimeStampIndex":
        return TimeStampIndex.build(self.dataset)

    def fetch_response(self, url, headers=None, auth=None):
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

    def flush(self) -> None:
        """Emitted entities are de-constructed into statements for the database
        to store. These are inserted in batches - so the statement cache on the
        context is flushed to the store. All statements that are not flushed
        when a crawl is aborted are not persisted to the database."""
        statements = list(self._statements.values())
        if self.dry_run:
            self.log.info(
                "Dry run: discarding %d statements..." % len(statements),
                entities=self.stats.entities,
                statements=self.stats.statements,
            )
            self._statements = {}
            return

        if len(statements):
            self.log.info(
                "Storing %d statements..." % len(statements),
                entities=self.stats.entities,
                statements=self.stats.statements,
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
        self.stats.entities += 1
        if target:
            self.stats.targets += 1
        for stmt in entity.statements:
            assert stmt.dataset == self.dataset.name, (
                stmt.prop,
                entity.dataset.name,
                stmt.dataset,
                self.dataset.name,
            )
            stmt.entity_id == entity.id
            stmt.canonical_id = canonical_id
            stmt.schema = entity.schema.name
            stmt.last_seen = settings.RUN_TIME_ISO
            self.stats.statements += 1
            if not self.dry_run:
                stmt.first_seen = self.timestamps.get(stmt.id)
            stmt.target = target or False
            stmt.external = external
            if stmt.lang is None and self.lang is not None:
                stmt.lang = self.lang
            # if not self.dry_run:
            self.sink.emit(stmt)
            self._statements[stmt.id] = stmt
        self.log.debug("Emitted", entity=entity.id, schema=entity.schema.name)
        if len(self._statements) >= (self.BATCH_SIZE * 10):
            self.flush()

    def crawl(self) -> bool:
        """Run the crawler."""
        if self.dataset.disabled:
            self.log.info("Dataset is disabled")
            return True
        self.begin(clear=True)
        self.log.info("Begin crawl", run_time=settings.RUN_TIME_ISO)
        try:
            # Run the dataset:
            method = load_entry_point(self.dataset)
            method(self)
            self.flush()
            if self.stats.entities == 0:
                self.log.warn(
                    "Crawler did not emit entities",
                    statements=self.stats.statements,
                )
            else:
                if not self.dry_run:
                    with engine_tx() as conn:
                        cleanup_dataset(conn, self.dataset)
            self.log.info(
                "Crawl completed",
                entities=self.stats.entities,
                statements=self.stats.statements,
            )
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
            self.resources.clear()
            if data:
                self.cache.clear()
                self.sink.clear()
                clear_statements(conn, self.dataset)
