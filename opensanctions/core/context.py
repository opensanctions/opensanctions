import json
import math
import asyncio
import hashlib
import aiofiles
import structlog
import mimetypes
from random import randint
from httpx import AsyncClient, Timeout, URL
from typing import Any, Dict, List, Optional, Tuple, Union
from lxml import etree, html
from pprint import pprint
from datetime import timedelta
from datapatch import LookupException
from lxml.etree import _Element, tostring
from followthemoney.util import make_entity_id
from followthemoney.schema import Schema

from opensanctions import settings
from opensanctions.core.http import HEADERS
from opensanctions.core.entity import Entity
from opensanctions.core.db import with_conn
from opensanctions.core.http import check_cache, save_cache, clear_cache
from opensanctions.core.issues import save_issue, clear_issues
from opensanctions.core.resources import save_resource, clear_resources
from opensanctions.core.statements import Statement, count_entities
from opensanctions.core.statements import cleanup_dataset, clear_statements
from opensanctions.core.statements import statements_from_entity, save_statements
from opensanctions.util import named_semaphore


class Context(object):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    SOURCE_TITLE = "Source data"
    BATCH_SIZE = 1000
    BATCH_CONCURRENT = 5

    def __init__(self, dataset):
        self.dataset = dataset
        self.path = settings.DATASET_PATH.joinpath(dataset.name)
        self.log = structlog.get_logger(
            dataset.name, dataset=self.dataset.name, _ctx=self
        )
        self._statements: List[Statement] = []
        self._events: List[Dict[str, Any]] = []
        self._http_client: Optional[AsyncClient] = None
        self.http_concurrency = 10

    async def begin(self) -> None:
        """Flush and tear down the context."""
        pass

    async def close(self) -> None:
        """Flush and tear down the context."""
        if len(self._events):
            async with with_conn() as conn:
                for event in self._events:
                    await save_issue(conn, event)

        if self._http_client is not None:
            await self._http_client.aclose()

    @property
    def http_client(self):
        if self._http_client is None:
            timeout = Timeout(settings.HTTP_TIMEOUT)
            self._http_client = AsyncClient(
                headers=HEADERS,
                timeout=timeout,
                follow_redirects=True,
            )
        return self._http_client

    def get_resource_path(self, name):
        return self.path.joinpath(name)

    async def fetch_resource(self, name, url):
        """Fetch a URL into a file located in the current run folder,
        if it does not exist."""
        file_path = self.get_resource_path(name)
        if not file_path.exists():
            self.log.info("Fetching resource", path=file_path.as_posix(), url=url)
            file_path.parent.mkdir(exist_ok=True, parents=True)
            async with aiofiles.open(file_path, "wb") as fh:
                async with self.http_client.stream("GET", url) as response:
                    async for chunk in response.aiter_bytes():
                        await fh.write(chunk)
        return file_path

    async def fetch_response(self, url):
        return await self.http_client.get(url)

    async def fetch_text(
        self,
        url,
        params=None,
        headers=None,
        auth=None,
        cache_days=None,
    ):
        url_ = URL(url, params=params)
        url = str(url_)

        if cache_days is not None:
            async with with_conn() as conn:
                min_cache = max(1, math.ceil(cache_days * 0.8))
                max_cache = math.ceil(cache_days * 1.2)
                cache_days = timedelta(days=randint(min_cache, max_cache))
                text = await check_cache(conn, url, cache_days)
                if text is not None:
                    self.log.debug("HTTP cache hit", url=url)
                    return text

        async with named_semaphore(f"http.{url_.host}", self.http_concurrency):
            self.log.debug("HTTP GET", url=url)
            response = await self.http_client.get(url, headers=headers, auth=auth)
            response.raise_for_status()
            text = response.text
            if text is None:
                return None
        async with with_conn() as conn:
            await save_cache(conn, url, self.dataset, text)
        return text

    async def fetch_json(self, url, **kwargs):
        """Fetch the given URL (GET) and decode it as a JSON object."""
        text = await self.fetch_text(url, **kwargs)
        if text is not None and len(text):
            return json.loads(text)

    async def fetch_html(self, url, **kwargs):
        text = await self.fetch_text(url, **kwargs)
        if text is not None and len(text):
            return html.fromstring(text)

    def parse_resource_xml(self, name):
        """Parse a file in the resource folder into an XML tree."""
        file_path = self.get_resource_path(name)
        with open(file_path, "rb") as fh:
            return etree.parse(fh)

    async def export_resource(self, path, mime_type=None, title=None):
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
        async with with_conn() as conn:
            return await save_resource(
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

    async def flush(self) -> None:
        """Emitted entities are de-constructed into statements for the database
        to store. These are inserted in batches - so the statement cache on the
        context is flushed to the store. All statements that are not flushed
        when a crawl is aborted are not persisted to the database."""
        while len(self._statements) > self.BATCH_SIZE:
            batch = self._statements[: self.BATCH_SIZE]
            self._statements = self._statements[self.BATCH_SIZE :]
            async with named_semaphore("stmt.upsert", self.BATCH_CONCURRENT):
                async with with_conn() as conn:
                    await save_statements(conn, batch)

    async def emit(
        self, entity: Entity, target: Optional[bool] = None, unique: bool = False
    ):
        """Send an FtM entity to the store."""
        if entity.id is None:
            raise ValueError("Entity has no ID: %r", entity)
        if target is not None:
            entity.target = target
        statements = statements_from_entity(entity, self.dataset, unique=unique)
        if not len(statements):
            raise ValueError("Entity has no properties: %r", entity)
        self._statements.extend(statements)
        self.log.debug("Emitted", entity=entity)
        await self.flush()

    async def crawl(self) -> None:
        """Run the crawler."""
        await self.begin()
        try:
            async with with_conn() as conn:
                await asyncio.gather(
                    clear_issues(conn, self.dataset),
                    clear_resources(conn, self.dataset),
                )
            self.log.info("Begin crawl")
            # Run the dataset:
            await self.dataset.method(self)
            await self.flush()
            async with with_conn() as conn:
                await cleanup_dataset(conn, self.dataset)
                entities = await count_entities(conn, dataset=self.dataset)
                targets = await count_entities(conn, dataset=self.dataset, target=True)

            self.log.info("Crawl completed", entities=entities, targets=targets)
        except KeyboardInterrupt:
            raise
        except LookupException as exc:
            self.log.error(exc.message, lookup=exc.lookup.name, value=exc.value)
        except Exception:
            self.log.exception("Crawl failed")
        finally:
            await self.close()

    async def clear(self) -> None:
        """Delete all recorded data for a given dataset."""
        async with with_conn() as conn:
            await asyncio.gather(
                clear_statements(conn, self.dataset),
                clear_issues(conn, self.dataset),
                clear_resources(conn, self.dataset),
                clear_cache(conn, self.dataset),
            )
