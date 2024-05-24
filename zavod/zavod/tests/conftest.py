import pytest
from pathlib import Path
from tempfile import mkdtemp, mkstemp
from nomenklatura import settings as nk_settings

from zavod import settings
from zavod.context import Context
from zavod.meta import get_catalog, load_dataset_from_path, Dataset
from zavod.dedupe import get_resolver

nk_settings.TESTING = True
settings.DATA_PATH = Path(mkdtemp()).resolve()
settings.RESOLVER_PATH = settings.DATA_PATH.joinpath("resolver.ijson").as_posix()
settings.ARCHIVE_BACKEND = "FileSystemBackend"
settings.ARCHIVE_PATH = settings.DATA_PATH / "test_archive"
settings.CACHE_DATABASE_URI = "sqlite:///:memory:"
settings.OPENSANCTIONS_API_KEY = "testkey"
settings.SYNC_POSITIONS = True
settings.ZYTE_API_KEY = "zyte-test-key"
FIXTURES_PATH = Path(__file__).parent / "fixtures"
DATASET_1_YML = FIXTURES_PATH / "testdataset1" / "testdataset1.yml"
DATASET_2_YML = FIXTURES_PATH / "testdataset2" / "testdataset2.yml"
DATASET_2_EXPORT_YML = FIXTURES_PATH / "testdataset2" / "testdataset2_export.yml"
DATASET_3_YML = FIXTURES_PATH / "testdataset3" / "testdataset3.yml"
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
def testdataset1() -> Dataset:
    dataset = load_dataset_from_path(DATASET_1_YML)
    assert dataset is not None
    return dataset


@pytest.fixture(scope="function")
def testdataset2() -> Dataset:
    dataset = load_dataset_from_path(DATASET_2_YML)
    assert dataset is not None
    return dataset


@pytest.fixture(scope="function")
def testdataset3() -> Dataset:
    dataset = load_dataset_from_path(DATASET_3_YML)
    assert dataset is not None
    return dataset


@pytest.fixture(scope="function")
def testdataset2_export() -> Dataset:
    dataset = load_dataset_from_path(DATASET_2_EXPORT_YML)
    assert dataset is not None
    return dataset


@pytest.fixture(scope="function")
def vcontext(testdataset1) -> Context:
    return Context(testdataset1)


@pytest.fixture(scope="function")
def analyzer(testdataset1) -> Dataset:
    assert testdataset1 is not None
    return load_dataset_from_path(ANALYZER_YML)


@pytest.fixture(scope="function")
def enricher(testdataset1) -> Dataset:
    assert testdataset1 is not None
    return load_dataset_from_path(ENRICHER_YML)


@pytest.fixture(scope="function")
def collection(testdataset1) -> Dataset:
    assert testdataset1 is not None
    return load_dataset_from_path(COLLECTION_YML)
