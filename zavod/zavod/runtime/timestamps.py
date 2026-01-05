import plyvel  # type: ignore
import shutil
from typing import Dict, Iterable
from rigour.env import ENCODING as E
from followthemoney import Statement

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.archive import dataset_state_path, iter_previous_statements

log = get_logger(__name__)


class TimeStampIndex(object):
    BUFFER = 10 * 1024 * 1024

    def __init__(self, dataset: Dataset) -> None:
        self.path = dataset_state_path(dataset.name) / "timestamps"
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

            # FIXME: Handle the migration of statement IDs from not including lang
            # to including lang. This is needed to read timestamps for statements
            # created before followthemoney 4.5.0. Once all timestamps have been
            # migrated, this block can be removed (est: March 2026).
            if stmt._lang is not None:
                new_id = stmt.generate_key()
                if new_id != stmt.id:
                    key = f"{stmt.entity_id}:{new_id}"
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
        # self.db.compact_range()
        log.info("Index ready.", count=total_size)

    @classmethod
    def build(cls, dataset: Dataset) -> "TimeStampIndex":
        index = cls(dataset)
        index.index(iter_previous_statements(dataset, external=False))
        return index

    def get(self, entity_id: str) -> Dict[str, str]:
        timestamps: Dict[str, str] = {}
        prefix = f"{entity_id}:".encode(E)
        with self.db.iterator(prefix=prefix) as it:
            for key, value in it:
                _, stmt_id = key.decode(E).split(":", 1)
                timestamps[stmt_id] = value.decode(E)
        return timestamps

    def close(self) -> None:
        self.db.close()
        shutil.rmtree(self.path.as_posix(), ignore_errors=True)

    def __hash__(self) -> int:
        return hash(self.db.name)

    def __repr__(self) -> str:
        return f"<TimeStampIndex({self.db.name!r})>"
