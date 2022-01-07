import asyncio
import hashlib
from sqlalchemy.ext.asyncio.engine import AsyncConnection, AsyncTransaction
import structlog
import mimetypes
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple, Union
from lxml import etree
from pprint import pprint
from datapatch import LookupException
from lxml.etree import _Element, tostring
from followthemoney.util import make_entity_id
from followthemoney.schema import Schema

from opensanctions import settings
from opensanctions.core.http import get_session, fetch_download
from opensanctions.core.entity import Entity
from opensanctions.core.resolver import get_resolver
from opensanctions.core.db import with_conn
from opensanctions.core.issues import save_issue, clear_issues
from opensanctions.core.resources import save_resource, clear_resources
from opensanctions.core.statements import Statement, count_entities
from opensanctions.core.statements import cleanup_dataset, clear_statements
from opensanctions.core.statements import statements_from_entity, save_statements


class Context(object):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    SOURCE_TITLE = "Source data"
    BATCH_SIZE = 1000

    def __init__(self, dataset):
        self.dataset = dataset
        self.path = settings.DATASET_PATH.joinpath(dataset.name)
        self.http = get_session()
        self.resolver = get_resolver()
        self.log = structlog.get_logger(
            dataset.name, dataset=self.dataset.name, _ctx=self
        )
        self._statements: Dict[Tuple[str, str, str], Statement] = {}
        self._events: List[Dict[str, Any]] = []

    async def begin(self) -> None:
        """Flush and tear down the context."""
        # self.conn = await engine.connect()
        pass

    async def close(self) -> None:
        """Flush and tear down the context."""
        if len(self._events):
            # if self.conn is None:
            #     self.conn = await engine.connect()
            async with with_conn() as conn:
                for event in self._events:
                    await save_issue(conn, event)
        # if self.conn is not None:
        #     await self.conn.close()
        #     self.conn = None

    # @asynccontextmanager
    # async def tx(self):
    #     tx = await self.conn.begin()
    #     try:
    #         yield tx
    #         await tx.commit()
    #     finally:
    #         await tx.close()

    # self.http.close()

    def get_resource_path(self, name):
        return self.path.joinpath(name)

    async def fetch_resource(self, name, url):
        """Fetch a URL into a file located in the current run folder,
        if it does not exist."""
        file_path = self.get_resource_path(name)
        if not file_path.exists():
            fetch_download(file_path, url)
        return file_path

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
        # if self.conn is None:
        #     raise RuntimeError("Not connected to DB")
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
        self.log.debug("Flushing statements to database...")
        # if self.conn is None:
        #     raise RuntimeError("No connection upon flush")
        async with with_conn() as conn:
            await save_statements(conn, list(self._statements.values()))
        self._statements = {}

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
        for stmt in statements:
            key = (stmt["entity_id"], stmt["prop"], stmt["value"])
            self._statements[key] = stmt
        if len(self._statements) >= self.BATCH_SIZE:
            await self.flush()
        self.log.debug("Emitted", entity=entity)

    async def crawl(self) -> None:
        """Run the crawler."""
        await self.begin()
        # if self.conn is None:
        #     raise RuntimeError("WTF")
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

                self.log.info(
                    "Crawl completed",
                    entities=await count_entities(conn, dataset=self.dataset),
                    targets=await count_entities(
                        conn, dataset=self.dataset, target=True
                    ),
                )
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
            )
