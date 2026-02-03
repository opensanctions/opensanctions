import importlib
from typing import Generator
import pytest
import shutil
from pathlib import Path
from tempfile import mkdtemp
import logging
from nomenklatura import Resolver
from nomenklatura.db import close_db

from zavod import settings
from zavod.context import Context
from zavod.entity import Entity
from zavod.logs import configure_logging, reset_logging
from zavod.meta import get_catalog, load_dataset_from_path, Dataset
from zavod.db import meta
from zavod.integration import get_resolver
from zavod.stateful.model import create_db

settings.nk.TESTING = True
settings.nk.DB_URL = "sqlite:///:memory:"
settings.DATA_PATH = Path(mkdtemp()).resolve()
settings.ARCHIVE_BACKEND = "FileSystemBackend"
settings.ARCHIVE_PATH = settings.DATA_PATH / "test_archive"
settings.ZYTE_API_KEY = "zyte-test-key"
FIXTURES_PATH = Path(__file__).parent / "fixtures"
DATASET_1_YML = FIXTURES_PATH / "testdataset1" / "testdataset1.yml"
DATASET_2_YML = FIXTURES_PATH / "testdataset2" / "testdataset2.yml"
DATASET_2_EXPORT_YML = FIXTURES_PATH / "testdataset2" / "testdataset2_export.yml"
DATASET_3_YML = FIXTURES_PATH / "testdataset3" / "testdataset3.yml"
DATASET_SECURITIES_YML = (
    FIXTURES_PATH / "testdataset_securities" / "testdataset_securities.yml"
)
DATASET_MARITIME_YML = (
    FIXTURES_PATH / "testdataset_maritime" / "testdataset_maritime.yml"
)
COLLECTION_YML = FIXTURES_PATH / "collection.yml"
XML_DOC = FIXTURES_PATH / "doc.xml"


def has_package(package_name):
    return importlib.util.find_spec(package_name) is not None


@pytest.fixture(autouse=True)
def wrap_test():
    shutil.rmtree(settings.ARCHIVE_PATH, ignore_errors=True)
    shutil.rmtree(settings.DATA_PATH, ignore_errors=True)
    settings.DATA_PATH = Path(mkdtemp()).resolve()
    create_db()
    yield
    get_catalog.cache_clear()
    close_db()
    meta.clear()


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
def testdataset_securities() -> Dataset:
    dataset = load_dataset_from_path(DATASET_SECURITIES_YML)
    assert dataset is not None
    return dataset


@pytest.fixture(scope="function")
def testdataset_maritime() -> Dataset:
    dataset = load_dataset_from_path(DATASET_MARITIME_YML)
    assert dataset is not None
    return dataset


@pytest.fixture(scope="function")
def testdataset_enrich_subject() -> Dataset:
    dataset = load_dataset_from_path(
        FIXTURES_PATH / "testdataset_enrich_subject" / "testdataset_enrich_subject.yml"
    )
    assert dataset is not None
    return dataset


@pytest.fixture(scope="function")
def testdataset_dedupe() -> Dataset:
    dataset = load_dataset_from_path(
        FIXTURES_PATH / "testdataset_dedupe" / "testdataset_dedupe.yml"
    )
    assert dataset is not None
    return dataset


@pytest.fixture(scope="function")
def vcontext(testdataset1) -> Generator[Context, None, None]:
    context = Context(testdataset1)
    yield context
    context.close()


@pytest.fixture(scope="function")
def analyzer(testdataset1) -> Dataset:
    assert testdataset1 is not None
    ds = load_dataset_from_path(FIXTURES_PATH / "analyzer.yml")
    assert ds is not None, "Failed to load analyzer dataset"
    return ds


@pytest.fixture(scope="function")
def enricher(testdataset1) -> Dataset:
    assert testdataset1 is not None
    ds = load_dataset_from_path(FIXTURES_PATH / "enricher.yml")
    assert ds is not None, "Failed to load enricher dataset"
    return ds


@pytest.fixture(scope="function")
def collection(testdataset1) -> Dataset:
    assert testdataset1 is not None
    ds = load_dataset_from_path(COLLECTION_YML)
    assert ds is not None, "Failed to load collection dataset"
    return ds


@pytest.fixture(scope="function")
def resolver() -> Generator[Resolver[Entity], None, None]:
    resolver = get_resolver()
    resolver.begin()
    yield resolver
    resolver.rollback()


@pytest.fixture(scope="function")
def logger() -> Generator[logging.Logger, None, None]:
    """Configure logging, and reset after test."""
    logger = configure_logging()
    yield logger
    reset_logging(logger)
