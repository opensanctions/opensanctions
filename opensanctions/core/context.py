import json
import hashlib
import mimetypes
from typing import Iterable, cast, Dict, Optional
from lxml import etree, html
from requests.exceptions import RequestException
from datapatch import LookupException
from sqlalchemy import MetaData
from zavod.context import GenericZavod
from followthemoney.helpers import check_person_cutoff
from structlog.contextvars import clear_contextvars, bind_contextvars
from nomenklatura.cache import Cache
from nomenklatura.util import normalize_url
from nomenklatura.judgement import Judgement
from nomenklatura.matching import compare_scored
from nomenklatura.resolver import Resolver
from nomenklatura.statement import Statement

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.core.db import engine, engine_tx
from opensanctions.core.external import External
from opensanctions.core.issues import clear_issues
from opensanctions.core.resolver import AUTO_USER
from opensanctions.core.resources import save_resource, clear_resources
from opensanctions.core.source import Source
from opensanctions.core.statements import count_entities
from opensanctions.core.statements import cleanup_dataset, clear_statements
from opensanctions.core.statements import save_statements


class Context(GenericZavod[Entity]):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    SOURCE_TITLE = "Source data"
    BATCH_SIZE = 5000
    BATCH_CONCURRENT = 5

    def __init__(self, dataset: Dataset):
        data_path = settings.DATASET_PATH.joinpath(dataset.name)
        super().__init__(
            dataset.name,
            Entity,
            prefix=dataset.prefix,
            data_path=data_path,
        )
        self.dataset = dataset
        self.source: Optional[Source] = dataset if isinstance(dataset, Source) else None
        self.cache = Cache(engine, MetaData(bind=engine), dataset)
        self._statements: Dict[str, Statement] = {}

    def bind(self) -> None:
        bind_contextvars(dataset=self.dataset.name)

    def close(self) -> None:
        """Flush and tear down the context."""
        super().close()
        clear_contextvars()

    def fetch_response(self, url, headers=None, auth=None):
        self.log.debug("HTTP GET", url=url)
        response = self.http.get(
            url,
            headers=headers,
            auth=auth,
            timeout=(settings.HTTP_TIMEOUT, settings.HTTP_TIMEOUT),
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

    def export_resource(self, path, mime_type=None, title=None):
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
        name = path.relative_to(self.path).as_posix()
        with engine_tx() as conn:
            return save_resource(
                conn, name, self.dataset, checksum, mime_type, size, title
            )

    def lookup_value(self, lookup, value, default=None, dataset=None):
        ds = Dataset.require(dataset) if dataset is not None else self.dataset
        try:
            return ds.lookups[lookup].get_value(value, default=default)
        except LookupException:
            return default

    def lookup(self, lookup, value, dataset=None):
        ds = Dataset.require(dataset) if dataset is not None else self.dataset
        return ds.lookups[lookup].match(value)

    def flush(self) -> None:
        """Emitted entities are de-constructed into statements for the database
        to store. These are inserted in batches - so the statement cache on the
        context is flushed to the store. All statements that are not flushed
        when a crawl is aborted are not persisted to the database."""
        statements = list(self._statements.values())
        with engine_tx() as conn:
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
        for stmt in entity.statements:
            stmt.dataset = self.dataset.name
            stmt.entity_id = entity.id
            stmt.canonical_id = entity.id
            stmt.schema = entity.schema.name
            stmt.first_seen = settings.RUN_TIME
            stmt.last_seen = settings.RUN_TIME
            stmt.target = target or False
            stmt.external = external
            if stmt.lang is None and self.source is not None:
                stmt.lang = self.source.data.lang
            stmt.id = Statement.make_key(
                stmt.dataset,
                stmt.entity_id,
                stmt.prop,
                stmt.value,
                external,
            )
            self._statements[stmt.id] = stmt
        self.log.debug("Emitted", entity=entity)
        if len(self._statements) >= (self.BATCH_SIZE * 10):
            self.flush()

    def crawl(self) -> bool:
        """Run the crawler."""
        self.bind()
        with engine_tx() as conn:
            clear_issues(conn, self.dataset)
        if self.source is None or self.source.disabled:
            self.log.info("Source is disabled")
            return False
        with engine_tx() as conn:
            clear_resources(conn, self.dataset)
        self.log.info("Begin crawl")
        try:
            # Run the dataset:
            self.source.method(self)
            self.flush()
            with engine_tx() as conn:
                cleanup_dataset(conn, self.dataset)
                entities = count_entities(conn, dataset=self.dataset)
                targets = count_entities(conn, dataset=self.dataset, target=True)

            self.log.info("Crawl completed", entities=entities, targets=targets)
            return True
        except KeyboardInterrupt:
            self.log.warning("Aborted by user (SIGINT)")
            return False
        except LookupException as lexc:
            self.log.error(lexc.message, lookup=lexc.lookup.name, value=lexc.value)
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

    def enrich(
        self,
        resolver: Resolver,
        entities: Iterable[Entity],
        threshold: Optional[float] = None,
    ):
        """Try to match a set of entities against an external source."""
        self.bind()
        with engine_tx() as conn:
            clear_issues(conn, self.dataset)
            clear_resources(conn, self.dataset)
            clear_statements(conn, self.dataset)
        external = cast(External, self.dataset)
        enricher = external.get_enricher(self.cache)
        try:
            for entity in entities:
                try:
                    for match in enricher.match_wrapped(entity):
                        judgement = resolver.get_judgement(match.id, entity.id)

                        # For unjudged candidates, compute a score and put it in the
                        # xref cache so the user can decide:
                        if judgement == Judgement.NO_JUDGEMENT:
                            if not entity.schema.can_match(match.schema):
                                continue
                            result = compare_scored(entity, match)
                            score = result["score"]
                            if threshold is None or score >= threshold:
                                self.log.info(
                                    "Match [%s]: %.2f -> %s" % (entity, score, match)
                                )
                                resolver.suggest(
                                    entity.id,
                                    match.id,
                                    score,
                                    user=AUTO_USER,
                                )

                        if judgement != Judgement.POSITIVE:
                            self.emit(match, external=True)

                        # Store previously confirmed matches to the database and make
                        # them visible:
                        if judgement == Judgement.POSITIVE:
                            self.log.info("Enrich [%s]: %r" % (entity, match))
                            for adjacent in enricher.expand_wrapped(entity, match):
                                if check_person_cutoff(adjacent):
                                    continue
                                # self.log.info("Added", entity=adjacent)
                                self.emit(adjacent)
                except Exception:
                    self.log.exception("Could not match: %r" % entity)
        except KeyboardInterrupt:
            pass
        finally:
            self.flush()
            enricher.close()
            self.close()

    def clear(self) -> None:
        """Delete all recorded data for a given dataset."""
        with engine_tx() as conn:
            clear_statements(conn, self.dataset)
            clear_issues(conn, self.dataset)
            clear_resources(conn, self.dataset)
        self.cache.clear()
