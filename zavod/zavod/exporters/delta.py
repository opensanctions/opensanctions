from typing import Any, Generator
import os
from collections import Counter

from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    Column,
    Integer,
    VARCHAR,
    PrimaryKeyConstraint,
)
from sqlalchemy.dialects.postgresql import insert

from zavod.entity import Entity
from zavod.archive import DELTA_EXPORT_FILE
from zavod.exporters.common import Exporter
from zavod.runtime.delta import HashDelta
from zavod.util import write_json

DB_URL = os.getenv(
    "OPENSANCTIONS_METRICS_DB_URI"
)  # "postgresql://postgres:password@localhost:10432/metrics"

meta = MetaData()
delta_counts = Table(
    "delta_counts",
    meta,
    Column("added", Integer),
    Column("dataset", VARCHAR),
    Column("run_id", VARCHAR),
    Column("modified", Integer),
    Column("deleted", Integer),
    PrimaryKeyConstraint("dataset", "run_id"),
)


class DeltaExporter(Exporter):
    TITLE = "Delta files"
    FILE_NAME = DELTA_EXPORT_FILE
    MIME_TYPE = "application/json"

    def setup(self) -> None:
        super().setup()
        self.delta = HashDelta(self.dataset)
        self.delta.backfill()
        self.db = None
        self.counts = {
            "ADD": 0,
            "MOD": 0,
            "DEL": 0,
        }
        if DB_URL:
            self.db = create_engine(DB_URL)
            meta.create_all(self.db)

    def feed(self, entity: Entity) -> None:
        self.delta.feed(entity)

    def generate(self) -> Generator[Any, None, None]:
        for op, entity_id in self.delta.generate():
            if op == "DEL":
                yield {"op": "DEL", "entity": {"id": entity_id}}
                continue
            entity = self.view.get_entity(entity_id)
            if entity is None:  # watman
                continue
            yield {"op": op, "entity": entity.to_dict()}

    def finish(self) -> None:
        with open(self.path, "wb") as fh:
            for op in self.generate():
                if op["op"] in self.counts:
                    self.counts[op["op"]] += 1
                write_json(op, fh)
        self.delta.close()
        if self.db:
            with self.db.connect() as conn:
                values = {
                    "added": self.counts["ADD"],
                    "modified": self.counts["MOD"],
                    "deleted": self.counts["DEL"],
                }
                stmt = insert(delta_counts).values(
                    dataset=self.dataset.name,
                    run_id=str(self.context.version),
                    **values
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["dataset", "run_id"], set_=values
                )
                conn.execute(stmt)
                conn.commit()

        super().finish()
