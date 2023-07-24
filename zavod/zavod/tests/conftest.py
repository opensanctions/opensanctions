import pytest
from pathlib import Path
from tempfile import mkdtemp, mkstemp

from zavod import settings
from zavod.meta import get_catalog, load_dataset_from_path, Dataset
from zavod.dedupe import get_resolver

settings.DATA_PATH = Path(mkdtemp()).resolve()
settings.RESOLVER_PATH = settings.DATA_PATH / "resolver.ijson"
settings.ARCHIVE_BUCKET = None
settings.CACHE_DATABASE_URI = None
FIXTURES_PATH = Path(__file__).parent / "fixtures"
VALIDATION_YML = FIXTURES_PATH / "validation" / "validation.yml"
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
    return load_dataset_from_path(VALIDATION_YML)


@pytest.fixture(scope="function")
def analyzer(vdataset) -> Dataset:
    assert vdataset is not None
    return load_dataset_from_path(ANALYZER_YML)


@pytest.fixture(scope="function")
def enricher(vdataset) -> Dataset:
    assert vdataset is not None
    return load_dataset_from_path(ENRICHER_YML)
