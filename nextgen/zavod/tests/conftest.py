import pytest
from pathlib import Path

from zavod import settings
from zavod.meta import get_catalog, load_dataset_from_path, Dataset

FIXTURES_PATH = Path(__file__).parent / "fixtures"
VALIDATION_YML = FIXTURES_PATH / "validation" / "validation.yml"
ANALYZER_YML = FIXTURES_PATH / "analyzer" / "analyzer.yml"


@pytest.fixture(autouse=True)
def clear_catalog():
    # catalog = get_catalog()
    yield
    get_catalog.cache_clear()


@pytest.fixture(scope="function")
def vdataset() -> Dataset:
    return load_dataset_from_path(VALIDATION_YML)


@pytest.fixture(scope="function")
def analyzer(vdataset) -> Dataset:
    assert vdataset is not None
    return load_dataset_from_path(ANALYZER_YML)
