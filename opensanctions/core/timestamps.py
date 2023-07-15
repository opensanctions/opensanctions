import plyvel
from typing import Iterable
from nomenklatura.statement import Statement

from zavod.logs import get_logger
from zavod.meta import Dataset
from opensanctions import settings
from opensanctions.core.statements import all_statements
from opensanctions.core.db import engine_read

log = get_logger(__name__)


class TimeStampIndex(object):
    def __init__(self, dataset: Dataset) -> None:
        path = settings.DATA_PATH / "timestamps" / dataset.name
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = plyvel.DB(path.as_posix(), create_if_missing=True)

    def index(self, statements: Iterable[Statement]) -> None:
        log.info("Building timestamp index...")
        # TODO: work out the previous logic.
        batch = self.db.write_batch()
        idx = 0
        for idx, stmt in enumerate(statements):
            if stmt.first_seen is not None:
                batch.put(stmt.id.encode("utf-8"), stmt.first_seen.encode("utf-8"))
        batch.write()
        log.info("Index ready.", count=idx)

    @classmethod
    def build(cls, dataset: Dataset) -> "TimeStampIndex":
        index = cls(dataset)
        with engine_read() as conn:
            statements = all_statements(conn, dataset, external=False)
            index.index(statements)
        return index

    def get(self, id: str) -> str:
        first_seen = self.db.get(id.encode("utf-8"))
        if first_seen is not None:
            return first_seen.decode("utf-8")
        return settings.RUN_TIME_ISO

    def __repr__(self) -> str:
        return f"<TimeStampIndex({self.db.name!r})>"
