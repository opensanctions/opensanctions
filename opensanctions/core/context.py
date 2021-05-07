import structlog
from lxml import etree
from datetime import datetime, timedelta
from structlog.contextvars import clear_contextvars, bind_contextvars
from followthemoney.cli.util import write_object

# from opensanctions.core import db
from opensanctions import settings
from opensanctions.core.entity import OSETLEntity
from opensanctions.core.collection import Collection
from opensanctions.core.http import get_session, fetch_download


class Context(object):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    def __init__(self, dataset):
        self.dataset = dataset
        self.path = settings.DATA_PATH.joinpath(dataset.name)
        self.store = dataset.store
        self._bulk = self.store.bulk()
        self.http = get_session(self.path)
        self.fragment = 0
        self.log = structlog.get_logger(dataset.name)

    def get_artifact_path(self, name):
        return self.path.joinpath(name)

    def fetch_artifact(self, name, url):
        """Fetch a URL into a file located in the current run folder,
        if it does not exist."""
        file_path = self.get_artifact_path(name)
        if not file_path.exists():
            fetch_download(file_path, url)
        return file_path

    def parse_artifact_xml(self, name):
        """Parse a file in the artifact folder into an XML tree."""
        file_path = self.get_artifact_path(name)
        with open(file_path, "rb") as fh:
            return etree.parse(fh)

    def lookup_value(self, lookup, value, default=None):
        return self.dataset.lookups[lookup].get_value(value, default=default)

    def make(self, schema):
        """Make a new entity with some dataset context set."""
        return OSETLEntity(self.dataset, schema)

    def emit(self, entity, target=False, unique=False):
        """Send an FtM entity to the store."""
        if entity.id is None:
            raise RuntimeError("Entity has no ID: %r", entity)
        # pprint(entity.to_dict())
        self.log.debug(
            entity,
            schema=entity.schema.name,
            id=entity.id,
            properties=entity.properties,
        )
        fragment = str(self.fragment)
        self._bulk.put(entity, fragment=fragment)
        self.fragment += 1

    def bind(self):
        bind_contextvars(dataset=self.dataset.name)

    def crawl(self):
        """Run the crawler."""
        try:
            self.bind()
            self.log.info("Begin crawl")
            # Run the dataset:
            self.dataset.method(self)
            self.log.info("Crawl completed", fragment=self.fragment)
        except Exception:
            self.log.exception("Crawl failed")
        finally:
            self.close()

    def _consolidate_collection(self):
        """Copy all of the entities from each source dataset into the combined
        store for the whole collection."""
        if not isinstance(self.dataset, Collection):
            return
        self.dataset.store.delete()
        for source in self.dataset.sources:
            if self.dataset == source:
                continue
            self.log.info(
                "Consolidating source entities into collection",
                source=source.name,
            )
            entities = list(source.store.iterate())
            self.log.info(
                "All entities in source",
                source=source.name,
                entities=len(entities),
            )
            bulk = self.dataset.store.bulk()
            for idx, entity in enumerate(entities, 1):
                bulk.put(entity, origin=source.name, fragment=idx)
            bulk.flush()

    def normalize(self):
        """Apply processing steps to the dataset which must be executed between
        the crawling and export steps."""
        try:
            self.bind()
            self._consolidate_collection()
        finally:
            self.close()

    def export(self):
        """Generate exported files for the dataset."""
        try:
            self.bind()

            ftm_path = self.get_artifact_path("entities.ftm.json")
            ftm_path.parent.mkdir(exist_ok=True, parents=True)
            self.log.info(
                "Writing entities to line-based JSON",
                path=ftm_path,
                entities=len(self.dataset.store),
            )
            with open(ftm_path, "w") as fh:
                for entity in self.dataset.store:
                    write_object(fh, entity)
        finally:
            self.close()

    def close(self):
        """Flush and tear down the context."""
        self._bulk.flush()
        clear_contextvars()

        # Explicitly clear HTTP cache:
        expire = timedelta(seconds=settings.CACHE_EXPIRE)
        expire_at = datetime.utcnow() - expire
        self.http.cache.remove_old_entries(expire_at)

        # Persist any events to the database:
        # db.session.commit()
