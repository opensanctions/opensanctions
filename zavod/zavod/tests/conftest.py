import pytest
from pathlib import Path
from tempfile import mkdtemp, mkstemp

from zavod import settings
from zavod.context import Context
from zavod.meta import get_catalog, load_dataset_from_path, Dataset
from zavod.dedupe import get_resolver

settings.DATA_PATH = Path(mkdtemp()).resolve()
settings.RESOLVER_PATH = settings.DATA_PATH.joinpath("resolver.ijson").as_posix()
settings.ARCHIVE_BUCKET = None
settings.CACHE_DATABASE_URI = None
FIXTURES_PATH = Path(__file__).parent / "fixtures"
DATASET_1_YML = FIXTURES_PATH / "testdataset1" / "testdataset1.yml"
DATASET_2_YML = FIXTURES_PATH / "testdataset2" / "testdataset2.yml"
COLLECTION_YML = FIXTURES_PATH / "collection.yml"
ANALYZER_YML = FIXTURES_PATH / "analyzer.yml"
ENRICHER_YML = FIXTURES_PATH / "enricher.yml"
XML_DOC = FIXTURES_PATH / "doc.xml"


@pytest.fixture(autouse=True)
def clear_catalog():
    _, path = mkstemp(suffix=".ijson")
    settings.RESOLVER_PATH = path
    get_resolver.cache_clear()
    yield
    get_catalog.cache_clear()


@pytest.fixture(scope="function")
def vdataset() -> Dataset:
    dataset = load_dataset_from_path(DATASET_1_YML)
    assert dataset is not None
    return dataset


@pytest.fixture(scope="function")
def vcontext(vdataset) -> Context:
    return Context(vdataset)


@pytest.fixture(scope="function")
def analyzer(vdataset) -> Dataset:
    assert vdataset is not None
    return load_dataset_from_path(ANALYZER_YML)


@pytest.fixture(scope="function")
def enricher(vdataset) -> Dataset:
    assert vdataset is not None
    return load_dataset_from_path(ENRICHER_YML)
