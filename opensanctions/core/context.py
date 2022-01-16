import json
import math
import hashlib
import requests
import structlog
import mimetypes
from random import randint
from typing import Any, Dict, List, Optional, Union
from lxml import etree, html
from pprint import pprint
from datetime import timedelta
from datapatch import LookupException
from lxml.etree import _Element, tostring
from followthemoney.util import make_entity_id
from followthemoney.schema import Schema

from opensanctions import settings
from opensanctions.core.entity import Entity
from opensanctions.core.db import engine_tx, engine_read
from opensanctions.core.http import check_cache, save_cache, clear_cache
from opensanctions.core.issues import save_issue, clear_issues
from opensanctions.core.resources import save_resource, clear_resources
from opensanctions.core.statements import Statement, count_entities
from opensanctions.core.statements import cleanup_dataset, clear_statements
from opensanctions.core.statements import statements_from_entity, save_statements
from opensanctions.util import normalize_url


class Context(object):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    SOURCE_TITLE = "Source data"
    BATCH_SIZE = 5000
    BATCH_CONCURRENT = 5

    def __init__(self, dataset):
        self.dataset = dataset
        self.path = settings.DATASET_PATH.joinpath(dataset.name)
        self.log = structlog.get_logger(
            dataset.name, dataset=self.dataset.name, _ctx=self
        )
        self._statements: Dict[str, Statement] = {}
        self._events: List[Dict[str, Any]] = []
        self.http = requests.Session()
        self.http.headers = dict(settings.HEADERS)

    def close(self) -> None:
        """Flush and tear down the context."""
        self.http.close()
        if len(self._events):
            with engine_tx() as conn:
                for event in self._events:
                    save_issue(conn, event)

    def get_resource_path(self, name):
        return self.path.joinpath(name)

    def fetch_resource(self, name, url):
        """Fetch a URL into a file located in the current run folder,
        if it does not exist."""
        file_path = self.get_resource_path(name)
        if not file_path.exists():
            self.log.info("Fetching resource", path=file_path.as_posix(), url=url)
            file_path.parent.mkdir(exist_ok=True, parents=True)
            with self.http.get(url, stream=True, timeout=settings.HTTP_TIMEOUT) as res:
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
            with engine_read() as conn:
                min_cache = max(1, math.ceil(cache_days * 0.8))
                max_cache = math.ceil(cache_days * 1.2)
                cache_days = timedelta(days=randint(min_cache, max_cache))
                text = check_cache(conn, url, cache_days)
                if text is not None:
                    self.log.debug("HTTP cache hit", url=url)
                    return text

        response = self.fetch_response(url, headers=headers, auth=auth)
        text = response.text
        if text is None:
            return None

        with engine_tx() as conn:
            save_cache(conn, url, self.dataset, text)
        return text

    def fetch_json(self, url, **kwargs):
        """Fetch the given URL (GET) and decode it as a JSON object."""
        text = self.fetch_text(url, **kwargs)
        if text is not None and len(text):
            return json.loads(text)

    def fetch_html(self, url, **kwargs):
        text = self.fetch_text(url, **kwargs)
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

    def lookup_value(self, lookup, value, default=None):
        try:
            return self.dataset.lookups[lookup].get_value(value, default=default)
        except LookupException:
            return default

    def lookup(self, lookup, value):
        return self.dataset.lookups[lookup].match(value)

    def make(self, schema: Union[str, Schema], target=False) -> Entity:
        """Make a new entity with some dataset context set."""
        return Entity(schema, target=target)

    def make_slug(self, *parts, strict=True) -> Optional[str]:
        return self.dataset.make_slug(*parts, strict=strict)

    def make_id(self, *parts: str) -> Optional[str]:
        hashed = make_entity_id(*parts, key_prefix=self.dataset.name)
        return self.make_slug(hashed)

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

    def emit(self, entity: Entity, target: Optional[bool] = None, unique: bool = False):
        """Send an FtM entity to the store."""
        if entity.id is None:
            raise ValueError("Entity has no ID: %r", entity)
        if target is not None:
            entity.target = target
        statements = statements_from_entity(entity, self.dataset, unique=unique)
        if not len(statements):
            raise ValueError("Entity has no properties: %r", entity)
        self._statements.update({s["id"]: s for s in statements})
        self.log.debug("Emitted", entity=entity)
        if len(self._statements) >= (self.BATCH_SIZE * 10):
            self.flush()

    def crawl(self) -> None:
        """Run the crawler."""
        try:
            with engine_tx() as conn:
                clear_issues(conn, self.dataset)
                clear_resources(conn, self.dataset)
            self.log.info("Begin crawl")
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
        except Exception:
            self.log.exception("Crawl failed")
        finally:
            self.close()

    def clear(self) -> None:
        """Delete all recorded data for a given dataset."""
        with engine_tx() as conn:
            clear_statements(conn, self.dataset)
            clear_issues(conn, self.dataset)
            clear_resources(conn, self.dataset)
            clear_cache(conn, self.dataset)
