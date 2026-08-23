from hashlib import sha1
from collections.abc import Generator

from followthemoney.dataset import Version
from nomenklatura import duck

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.archive import (
    HASH_FILE,
    backfill_artifact,
    dataset_artifact_path,
    get_last_successful_version,
)

log = get_logger(__name__)

BATCH_SIZE = 1_000


class HashDelta:
    """Classify the entities of a run as added or modified against the last
    successful run, and find the ones deleted since.

    Entities are fed during the export traversal and classified in batches
    (one DuckDB lookup against the previous run's hash file per batch), so the
    exporter receives (op, entity) pairs while it still holds the assembled
    entity - nothing is re-fetched from the store. Deletions are an anti-join
    between the two hash files at the end; they only need ids."""

    def __init__(self, dataset: Dataset, current: Version):
        self.dataset = dataset
        self.curr = current
        self.prev = get_last_successful_version(self.dataset.name)
        self.curr_path = dataset_artifact_path(dataset.name, self.curr, HASH_FILE)
        self.fh = self.curr_path.open("w")
        self.conn = duck.connect()
        self.conn.execute("CREATE TABLE prev_hashes (id VARCHAR, hash VARCHAR)")
        self._batch: list[tuple[str, str, Entity]] = []

    def _read_hashes_sql(self, path_sql: str) -> str:
        return (
            f"read_csv('{path_sql}', delim = ':', quote = '', header = false, "
            "columns = {'id': 'VARCHAR', 'hash': 'VARCHAR'})"
        )

    def backfill(self) -> None:
        if self.prev is None or self.prev == self.curr:
            log.info("No previous version found, skipping backfill.")
            return
        path = backfill_artifact(self.dataset.name, self.prev, HASH_FILE)
        if path is None:
            log.info(
                "No previous hash file found, skipping backfill.",
                version=self.prev.id,
            )
            return
        log.info("Loading previous hashes...", version=self.prev.id)
        if path.stat().st_size > 0:
            path_sql = str(path).replace("'", "''")
            self.conn.execute(
                "INSERT INTO prev_hashes "
                f"SELECT id, hash FROM {self._read_hashes_sql(path_sql)}"
            )
        self.conn.execute("CREATE INDEX prev_hashes_id ON prev_hashes (id)")

    def feed(self, entity: Entity) -> list[tuple[str, Entity]]:
        """Hash and buffer one traversal entity. Returns the classified
        (op, entity) pairs of a full batch, usually an empty list."""
        if entity.id is None:
            return []
        digest = sha1()
        digest.update(entity.id.encode("utf-8"))
        digest.update(entity.schema.name.encode("utf-8"))
        for prop, values in sorted(entity.properties.items()):
            digest.update(prop.encode("utf-8"))
            for value in sorted(values):
                digest.update(value.encode("utf-8"))
        entity_hash = digest.hexdigest()
        self.fh.write(f"{entity.id}:{entity_hash}\n")
        self._batch.append((entity.id, entity_hash, entity))
        if len(self._batch) >= BATCH_SIZE:
            return self.flush()
        return []

    def flush(self) -> list[tuple[str, Entity]]:
        """Classify the buffered entities against the previous run's hashes."""
        if len(self._batch) == 0:
            return []
        ids = [entity_id for entity_id, _, _ in self._batch]
        holes = ", ".join("?" for _ in ids)
        rows = self.conn.execute(
            f"SELECT id, hash FROM prev_hashes WHERE id IN ({holes})", ids
        ).fetchall()
        prev_hashes: dict[str, str] = dict(rows)
        ops: list[tuple[str, Entity]] = []
        for entity_id, entity_hash, entity in self._batch:
            prev_hash = prev_hashes.get(entity_id)
            if prev_hash is None:
                ops.append(("ADD", entity))
            elif prev_hash != entity_hash:
                ops.append(("MOD", entity))
        self._batch = []
        return ops

    def deletions(self) -> Generator[str, None, None]:
        """Ids seen in the previous run but not in this one. Call after the
        last flush(), when the current hash file is complete."""
        self.fh.flush()
        query = "SELECT DISTINCT id FROM prev_hashes"
        if self.curr_path.stat().st_size > 0:
            path_sql = str(self.curr_path).replace("'", "''")
            query += f" ANTI JOIN {self._read_hashes_sql(path_sql)} curr USING (id)"
        cursor = self.conn.execute(query)
        while rows := cursor.fetchmany(BATCH_SIZE):
            for row in rows:
                yield str(row[0])

    def close(self) -> None:
        self.fh.close()
        self.conn.close()
