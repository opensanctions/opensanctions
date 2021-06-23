import hashlib
import mimetypes
import structlog
from lxml import etree
from datapatch import LookupException
from structlog.contextvars import clear_contextvars, bind_contextvars

from opensanctions import settings
from opensanctions.model import db, Statement, Issue, Resource
from opensanctions.core.entity import Entity
from opensanctions.core.http import get_session, fetch_download
from opensanctions.core.export import export_dataset


class Context(object):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    def __init__(self, dataset):
        self.dataset = dataset
        self.path = settings.DATASET_PATH.joinpath(dataset.name)
        self.http = get_session()
        self.log = structlog.get_logger(dataset.name)

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
        checksum = digest.hexdigest()
        rel_path = path.relative_to(self.path).as_posix()
        Resource.save(rel_path, self.dataset, checksum, mime_type, size, title)

    def lookup_value(self, lookup, value, default=None):
        try:
            return self.dataset.lookups[lookup].get_value(value, default=default)
        except LookupException:
            return default

    def lookup(self, lookup, value):
        return self.dataset.lookups[lookup].match(value)

    def make(self, schema):
        """Make a new entity with some dataset context set."""
        return Entity(self.dataset, schema)

    def make_sanction(self, entity, key=None):
        """Create a sanctions object derived from the dataset metadata."""
        assert entity.schema.is_a("Thing"), entity.schema
        sanction = self.make("Sanction")
        sanction.make_id("Sanction", entity.id, key)
        sanction.add("entity", entity)
        publisher = self.dataset.publisher
        if publisher.country != "zz":
            sanction.add("country", publisher.country)
        sanction.add("authority", publisher.authority)
        if not sanction.has("authority"):
            sanction.add("authority", publisher.organization)
        sanction.add("sourceUrl", self.dataset.url)
        return sanction

    def emit(self, entity, target=False, unique=False):
        """Send an FtM entity to the store."""
        if entity.id is None:
            raise RuntimeError("Entity has no ID: %r", entity)
        self.log.debug("Emitted", entity=entity)
        Statement.from_entity(entity, target=target, unique=unique)

    def bind(self):
        bind_contextvars(dataset=self.dataset.name)

    def crawl(self):
        """Run the crawler."""
        try:
            self.bind()
            Issue.clear(self.dataset)
            self.log.info("Begin crawl")
            # Run the dataset:
            self.dataset.method(self)
            self.log.info(
                "Crawl completed",
                entities=Statement.all_counts(dataset=self.dataset),
                targets=Statement.all_counts(dataset=self.dataset, target=True),
            )
        except LookupException as exc:
            self.log.error(exc.message, lookup=exc.lookup.name, value=exc.value)
        except Exception:
            db.session.rollback()
            self.log.exception("Crawl failed")
        finally:
            self.close()

    def export(self):
        """Generate exported files for the dataset."""
        try:
            self.bind()
            export_dataset(self, self.dataset)
        finally:
            self.close()

    def clear(self):
        """Delete all recorded data for a given dataset."""
        Issue.clear(self.dataset)
        Statement.clear(self.dataset)
        db.session.commit()

    def get_entity(self, entity_id):
        for entity in Entity.query(self.dataset, entity_id=entity_id):
            return entity

    def close(self):
        """Flush and tear down the context."""
        clear_contextvars()

        db.session.commit()
