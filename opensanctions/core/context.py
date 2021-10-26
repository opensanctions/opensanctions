import hashlib
import mimetypes
from typing import Optional, Union
import structlog
from lxml import etree
from pprint import pprint
from datapatch import LookupException
from lxml.etree import _Element, tostring
from structlog.contextvars import clear_contextvars, bind_contextvars
from followthemoney.util import make_entity_id
from followthemoney.schema import Schema

from opensanctions import settings
from opensanctions.model import db, Statement, Issue, Resource
from opensanctions.core.http import get_session, fetch_download
from opensanctions.core.entity import Entity
from opensanctions.core.resolver import get_resolver


class Context(object):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    SOURCE_TITLE = "Source data"

    def __init__(self, dataset):
        self.dataset = dataset
        self.path = settings.DATASET_PATH.joinpath(dataset.name)
        self.http = get_session()
        self.resolver = get_resolver()
        self.log = structlog.get_logger(dataset.name)
        self._statements = {}

    def get_resource_path(self, name):
        return self.path.joinpath(name)

    def fetch_resource(self, name, url):
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
        return Resource.save(name, self.dataset, checksum, mime_type, size, title)

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
        self.log.debug("Flushing statements to database...")
        Statement.upsert_many(list(self._statements.values()))
        self._statements = {}

    def emit(self, entity: Entity, target: Optional[bool] = None, unique: bool = False):
        """Send an FtM entity to the store."""
        if entity.id is None:
            raise ValueError("Entity has no ID: %r", entity)
        if target is not None:
            entity.target = target
        statements = Statement.from_entity(
            entity, self.dataset, self.resolver, unique=unique
        )
        if not len(statements):
            raise ValueError("Entity has no properties: %r", entity)
        for stmt in statements:
            key = (stmt["entity_id"], stmt["prop"], stmt["value"])
            self._statements[key] = stmt
        if len(self._statements) >= db.batch_size:
            self.flush()
        self.log.debug("Emitted", entity=entity)

    def bind(self) -> None:
        bind_contextvars(dataset=self.dataset.name)

    def crawl(self) -> None:
        """Run the crawler."""
        try:
            self.bind()
            Issue.clear(self.dataset)
            Resource.clear(self.dataset)
            db.session.commit()
            self.log.info("Begin crawl")
            # Run the dataset:
            self.dataset.method(self)
            self.flush()
            Statement.cleanup_dataset(self.dataset)
            self.log.info(
                "Crawl completed",
                entities=Statement.all_counts(dataset=self.dataset),
                targets=Statement.all_counts(dataset=self.dataset, target=True),
            )
        except KeyboardInterrupt:
            db.session.rollback()
            raise
        except LookupException as exc:
            db.session.rollback()
            self.log.error(exc.message, lookup=exc.lookup.name, value=exc.value)
        except Exception:
            db.session.rollback()
            self.log.exception("Crawl failed")
        finally:
            self.close()

    def clear(self) -> None:
        """Delete all recorded data for a given dataset."""
        Issue.clear(self.dataset)
        Statement.clear(self.dataset)
        db.session.commit()

    def close(self) -> None:
        """Flush and tear down the context."""
        clear_contextvars()
        db.session.commit()
