import structlog
from lxml import etree
from datapatch import LookupException
from structlog.contextvars import clear_contextvars, bind_contextvars
from followthemoney import model
from followthemoney.cli.util import write_object

from opensanctions import settings
from opensanctions.model import db, Statement, Issue
from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.core.http import get_session, fetch_download
from opensanctions.core.export import write_json


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
        try:
            return self.dataset.lookups[lookup].get_value(value, default=default)
        except LookupException:
            return default

    def lookup(self, lookup, value):
        return self.dataset.lookups[lookup].match(value)

    def make(self, schema):
        """Make a new entity with some dataset context set."""
        return Entity(self.dataset, schema)

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
            self.log.exception("Crawl failed")
        finally:
            self.close()

    def export(self):
        """Generate exported files for the dataset."""
        try:
            self.bind()

            ftm_path = self.get_artifact_path("entities.ftm.json")
            ftm_path.parent.mkdir(exist_ok=True, parents=True)
            self.log.info("Writing entities to line-based JSON", path=ftm_path)
            with open(ftm_path, "w") as fh:
                for entity in Entity.query(self.dataset):
                    write_object(fh, entity)

            meta_path = self.get_artifact_path("metadata.json")
            with open(meta_path, "w") as fh:
                meta = self.dataset.to_metadata(shallow=False)
                write_json(meta, fh)
        finally:
            self.close()

    @classmethod
    def export_global_metadata(cls):
        """Export the global metadata for all datasets.

        This should live somewhere else."""
        meta_path = settings.DATASET_PATH.joinpath("metadata.json")
        datasets = []
        for dataset in Dataset.all():
            datasets.append(dataset.to_metadata(shallow=True))

        with open(meta_path, "w") as fh:
            meta = {"datasets": datasets, "model": model}
            write_json(meta, fh)

    def get_entity(self, entity_id):
        for entity in Entity.query(self.dataset, entity_id=entity_id):
            return entity

    def close(self):
        """Flush and tear down the context."""
        clear_contextvars()

        db.session.commit()
