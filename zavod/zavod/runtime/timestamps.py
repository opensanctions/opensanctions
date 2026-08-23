import plyvel  # type: ignore
from collections.abc import Iterable
from rigour.env import ENCODING as E
from followthemoney import Statement
from followthemoney.dataset import Version

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.archive import (
    dataset_state_path,
    get_last_successful_version,
    stream_statements,
)

log = get_logger(__name__)


class TimeStampIndex:
    """Lookup of the first_seen timestamp for every statement in the last
    successful run of a dataset.

    The index is a pure function of that version, so it is kept as a cache
    keyed by the version id and rebuilt only when a new version has been
    published - not on every crawl."""

    BUFFER = 10 * 1024 * 1024
    DONE_KEY = b"$done"

    def __init__(self, dataset: Dataset, version: Version | None) -> None:
        base = dataset_state_path(dataset.name) / "timestamps"
        base.mkdir(parents=True, exist_ok=True)
        name = version.id if version is not None else "none"
        self.path = base / name
        self.db = plyvel.DB(
            self.path.as_posix(),
            create_if_missing=True,
            write_buffer_size=self.BUFFER,
            lru_cache_size=self.BUFFER,
        )

    def index(self, statements: Iterable[Statement]) -> None:
        log.info("Building timestamp index...")
        batch = self.db.write_batch()
        batch_size = 0
        total_size = 0
        for stmt in statements:
            if stmt.first_seen is None or stmt.id is None or stmt.entity_id is None:
                continue
            if len(stmt.first_seen.strip()) == 0:
                continue
            key = f"{stmt.entity_id}:{stmt.id}"
            batch.put(key.encode(E), stmt.first_seen.encode(E))
            batch_size += 1
            total_size += 1

            if batch_size > 0 and batch_size % 500_000 == 0:
                batch.write()
                batch.clear()
                batch = self.db.write_batch()
                batch_size = 0

        batch.write()
        batch.clear()
        log.info("Index ready.", count=total_size)

    @classmethod
    def build(cls, dataset: Dataset) -> "TimeStampIndex":
        version = get_last_successful_version(dataset.name)
        index = cls(dataset, version)
        if index.db.get(cls.DONE_KEY) is not None:
            log.info(
                "Using cached timestamp index.",
                version=None if version is None else version.id,
            )
            return index
        if version is not None:
            index.index(stream_statements(dataset.name, version, external=False))
        index.db.put(cls.DONE_KEY, b"1")
        return index

    def get(self, entity_id: str) -> dict[str, str]:
        timestamps: dict[str, str] = {}
        prefix = f"{entity_id}:".encode(E)
        with self.db.iterator(prefix=prefix) as it:
            for key, value in it:
                _, stmt_id = key.decode(E).split(":", 1)
                timestamps[stmt_id] = value.decode(E)
        return timestamps

    def close(self) -> None:
        self.db.close()

    def __hash__(self) -> int:
        return hash(self.db.name)

    def __repr__(self) -> str:
        return f"<TimeStampIndex({self.db.name!r})>"
