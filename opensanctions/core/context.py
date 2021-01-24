import json
import structlog
from lxml import etree, html
from datetime import datetime
from ftmstore import get_dataset
from followthemoney import model

from opensanctions import settings
from opensanctions.core.http import get_session, fetch_download
from opensanctions.core.logs import clear_contextvars, bind_contextvars


class Context(object):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    def __init__(self, dataset):
        self.run_id = datetime.utcnow().strftime("%Y%m%d")
        self.dataset = dataset
        self.store = dataset.store
        self._bulk = self.store.bulk()
        self.http = get_session()
        self.fragment = 0
        self.log = structlog.get_logger(dataset.name)

    @property
    def path(self):
        path = settings.DATA_PATH
        path = path.joinpath(self.dataset.name)
        return path.joinpath(self.run_id)

    def fetch_artifact(self, name, url):
        """Fetch a URL into a file located in the current run folder,
        if it does not exist."""
        file_path = self.path.joinpath(name)
        if not file_path.exists():
            fetch_download(file_path, url)
        return file_path

    def parse_artifact_xml(self, name):
        """Parse a file in the artifact folder into an XML tree."""
        file_path = self.path.joinpath(name)
        with open(file_path, "rb") as fh:
            return etree.parse(fh)

    def make(self, schema):
        """Make a new entity with some dataset context set."""
        return model.make_entity(schema, key_prefix=self.dataset.name)

    def emit(self, entity):
        """Send an FtM entity to the store."""
        if entity.id is None:
            raise RuntimeError("Entity has no ID: %r", entity)
        # pprint(entity.to_dict())
        # self.log.debug(entity, schema=entity.schema.name, id=entity.id)
        fragment = str(self.fragment)
        self._bulk.put(entity, fragment=fragment)
        self.fragment += 1

    def bind(self):
        bind_contextvars(dataset=self.dataset.name, run_id=self.run_id)

    def crawl(self):
        """Run the crawler."""
        try:
            self.bind()
            self.log.info("Begin crawl")
            # Run the dataset:
            self.dataset.method(self)
            self.log.info("Crawl completed")
        except Exception:
            self.log.exception("Crawl failed")
        finally:
            self.close()

    def close(self):
        """Flush and tear down the context."""
        self._bulk.flush()
        clear_contextvars()
