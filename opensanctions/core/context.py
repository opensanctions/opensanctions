import json
import hashlib
import requests
import structlog
import mimetypes
from typing import Iterable, cast, Dict, Optional, Union
from lxml import etree, html
from pprint import pprint
from datapatch import LookupException
from sqlalchemy import MetaData
from lxml.etree import _Element, tostring
from followthemoney import model
from followthemoney.util import make_entity_id
from followthemoney.schema import Schema
from structlog.contextvars import clear_contextvars, bind_contextvars
from nomenklatura.cache import Cache
from nomenklatura.util import normalize_url
from nomenklatura.judgement import Judgement
from nomenklatura.matching import compare_scored
from nomenklatura.resolver import Resolver

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.core.db import engine, engine_tx
from opensanctions.core.external import External
from opensanctions.core.issues import clear_issues
from opensanctions.core.resources import save_resource, clear_resources
from opensanctions.core.statements import Statement
from opensanctions.core.statements import count_entities
from opensanctions.core.statements import cleanup_dataset, clear_statements
from opensanctions.core.statements import statements_from_entity, save_statements


class Context(object):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    SOURCE_TITLE = "Source data"
    BATCH_SIZE = 5000
    BATCH_CONCURRENT = 5

    def __init__(self, dataset: Dataset):
        self.dataset = dataset
        self.path = settings.DATASET_PATH.joinpath(dataset.name)
        self.log: structlog.stdlib.BoundLogger = structlog.get_logger(dataset.name)
        self.cache = Cache(engine, MetaData(bind=engine), dataset)
        self._statements: Dict[str, Statement] = {}
        self.http = requests.Session()
        self.http.headers.update(settings.HEADERS)

    def bind(self) -> None:
        bind_contextvars(dataset=self.dataset.name)

    def close(self) -> None:
        """Flush and tear down the context."""
        self.http.close()
        clear_contextvars()

    def get_resource_path(self, name):
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path.joinpath(name)

    def fetch_resource(self, name, url, auth=None, headers=None):
        """Fetch a URL into a file located in the current run folder,
        if it does not exist."""
        file_path = self.get_resource_path(name)
        if not file_path.exists():
            self.log.info("Fetching resource", path=file_path.as_posix(), url=url)
            file_path.parent.mkdir(exist_ok=True, parents=True)
            with self.http.get(
                url,
                stream=True,
                auth=auth,
                headers=headers,
                timeout=settings.HTTP_TIMEOUT,
                verify=False,
            ) as res:
                res.raise_for_status()
                with open(file_path, "wb") as handle:
                    for chunk in res.iter_content(chunk_size=8192 * 16):
                        handle.write(chunk)
        return file_path

    def fetch_response(self, url, headers=None, auth=None):
        self.log.debug("HTTP GET", url=url)
        response = self.http.get(
            url,
            headers=headers,
            auth=auth,
            timeout=settings.HTTP_TIMEOUT,
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

    def make(self, schema: Union[str, Schema], target=False) -> Entity:
        """Make a new entity with some dataset context set."""
        return Entity(model, {"schema": schema, "target": target})

    def make_slug(
        self, *parts, strict: bool = True, dataset: Optional[str] = None
    ) -> Optional[str]:
        ds = Dataset.require(dataset) if dataset is not None else self.dataset
        return ds.make_slug(*parts, strict=strict)

    def make_id(self, *parts: str, dataset: Optional[str] = None) -> Optional[str]:
        hashed = make_entity_id(*parts, key_prefix=self.dataset.name)
        return self.make_slug(hashed, dataset=dataset)

    def pprint(self, obj) -> None:
        """Utility to avoid dumb imports."""
        if isinstance(obj, _Element):
            obj = tostring(obj, pretty_print=True, encoding=str)
            print(obj)
        else:
            pprint(obj)

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
        if target is not None:
            entity.target = target
        statements = statements_from_entity(entity, self.dataset, external=external)
        if not len(statements):
            raise ValueError("Entity has no properties: %r", entity)
        self._statements.update({s["id"]: s for s in statements})
        self.log.debug("Emitted", entity=entity)
        if len(self._statements) >= (self.BATCH_SIZE * 10):
            self.flush()

    def crawl(self) -> None:
        """Run the crawler."""
        self.bind()
        with engine_tx() as conn:
            clear_issues(conn, self.dataset)
        if self.dataset.disabled:
            self.log.info("Source is disabled")
            return
        with engine_tx() as conn:
            clear_resources(conn, self.dataset)
        self.log.info("Begin crawl")
        try:
            # Run the dataset:
            self.dataset.method(self)
            self.flush()
            with engine_tx() as conn:
                cleanup_dataset(conn, self.dataset)
                entities = count_entities(conn, dataset=self.dataset)
                targets = count_entities(conn, dataset=self.dataset, target=True)

            self.log.info("Crawl completed", entities=entities, targets=targets)
        except KeyboardInterrupt:
            raise
        except LookupException as exc:
            self.log.error(exc.message, lookup=exc.lookup.name, value=exc.value)
            raise
        except Exception:
            self.log.exception("Crawl failed")
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
        from opensanctions.helpers.constraints import check_person_cutoff

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
                                resolver.suggest(entity.id, match.id, score)

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
