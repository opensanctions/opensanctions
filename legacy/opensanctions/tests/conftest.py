import pytest
from pathlib import Path
from tempfile import mkdtemp, mkstemp

from zavod import settings
from zavod.meta import load_dataset_from_path, Dataset

settings.DATA_PATH = Path(mkdtemp()).resolve()
settings.RESOLVER_PATH = settings.DATA_PATH / "resolver.ijson"
settings.ARCHIVE_BUCKET = None
settings.CACHE_DATABASE_URI = None
FIXTURES_PATH = Path(__file__).parent.parent.parent.parent / "zavod" / "zavod" / "tests" / "fixtures"
VALIDATION_YML = FIXTURES_PATH / "validation" / "validation.yml"


@pytest.fixture(scope="function")
def vdataset() -> Dataset:
    dataset = load_dataset_from_path(VALIDATION_YML)
    assert dataset is not None
    return dataset
