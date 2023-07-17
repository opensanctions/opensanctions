from functools import cache
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Engine
from nomenklatura.cache import Cache

from zavod import settings
from zavod.meta import Dataset
from zavod.archive import dataset_state_path


@cache
def get_engine(uri: str) -> Engine:
    """Get a SQLAlchemy engine for the given database URI."""
    return create_engine(uri)


@cache
def get_metadata(uri: str) -> MetaData:
    """Get a SQLAlchemy metadata cache for the given database URI."""
    assert uri is not None
    return MetaData()


@cache
def get_cache(dataset: Dataset) -> Cache:
    """Get a cache object for the given dataset."""
    database_uri = settings.CACHE_DATABASE_URI
    if database_uri is None:
        cache_path = dataset_state_path(dataset.name) / "cache.sqlite3"
        database_uri = f"sqlite:///{cache_path.as_posix()}"
    engine = get_engine(database_uri)
    metadata = get_metadata(database_uri)
    return Cache(engine, metadata, dataset, create=True)
