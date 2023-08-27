import plyvel  # type: ignore
from typing import Iterable, Optional
from nomenklatura.statement import Statement

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.archive import dataset_state_path, iter_previous_statements

log = get_logger(__name__)


class TimeStampIndex(object):
    def __init__(self, dataset: Dataset) -> None:
        path = dataset_state_path(dataset.name) / "timestamps"
        self.db = plyvel.DB(path.as_posix(), create_if_missing=True)

    def index(self, statements: Iterable[Statement]) -> None:
        log.info("Building timestamp index...")
        batch = self.db.write_batch()
        idx = 0
        for idx, stmt in enumerate(statements):
            if stmt.first_seen is None or stmt.id is None:
                continue
            if len(stmt.first_seen.strip()) == 0:
                continue
            batch.put(stmt.id.encode("utf-8"), stmt.first_seen.encode("utf-8"))
            if idx > 0 and idx % 1_000_000 == 0:
                batch.write()
        batch.write()
        log.info("Index ready.", count=idx)

    @classmethod
    def build(cls, dataset: Dataset) -> "TimeStampIndex":
        index = cls(dataset)
        index.index(iter_previous_statements(dataset, external=False))
        return index

    def get(self, id: Optional[str], default: str) -> str:
        if id is None:
            return default
        first_seen: Optional[bytes] = self.db.get(id.encode("utf-8"))
        if first_seen is not None:
            return first_seen.decode("utf-8")
        return default

    def close(self) -> None:
        self.db.close()

    def __repr__(self) -> str:
        return f"<TimeStampIndex({self.db.name!r})>"
