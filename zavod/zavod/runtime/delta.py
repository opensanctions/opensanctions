from hashlib import sha1
import shutil
import plyvel  # type: ignore
from typing import Optional, Generator, Tuple
from nomenklatura.versions import Version

from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.archive import dataset_resource_path, dataset_state_path
from zavod.archive import iter_dataset_versions, get_artifact_object, HASH_FILE
from zavod.runtime.versions import get_latest

log = get_logger(__name__)


class HashDelta(object):
    def __init__(self, dataset: Dataset):
        self.dataset = dataset
        self.curr = get_latest(dataset.name, backfill=False)
        self.prev: Optional[Version] = None
        self.curr_path = dataset_resource_path(dataset.name, HASH_FILE)
        self.fh = self.curr_path.open("w")
        self.db_path = dataset_state_path(dataset.name) / "hashes"
        shutil.rmtree(self.db_path, ignore_errors=True)
        self.db = plyvel.DB(self.db_path.as_posix(), create_if_missing=True)

    def backfill(self) -> None:
        for version in iter_dataset_versions(self.dataset.name):
            obj = get_artifact_object(self.dataset.name, HASH_FILE, version.id)
            if obj is None or version == self.curr:
                continue
            self.prev = version
            log.info(
                "Loading previous hashes...",
                version=version.id,
            )
            with obj.open() as fh:
                for line in fh:
                    entity_id, entity_hash = line.strip().split(":", 1)
                    key = f"{entity_id}:{version.id}".encode("utf-8")
                    self.db.put(key, entity_hash.encode("utf-8"))
            return
        log.info("No previous hash data found.")

    def feed(self, entity: Entity) -> None:
        if entity.id is None or self.curr is None:
            return
        digest = sha1()
        digest.update(entity.id.encode("utf-8"))
        digest.update(entity.schema.name.encode("utf-8"))
        for prop, values in sorted(entity.properties.items()):
            digest.update(prop.encode("utf-8"))
            for value in sorted(values):
                digest.update(value.encode("utf-8"))
        entity_hash = digest.hexdigest()
        # entity_hash = hash_data((entity.id, entity.schema.name, entity.properties))
        # assert entity_hash == digest.hexdigest(), (
        #     f"Hash mismatch for {entity.id}: {entity_hash} != {digest.hexdigest()}"
        # )
        self.fh.write(f"{entity.id}:{entity_hash}\n")
        key = f"{entity.id}:{self.curr.id}".encode("utf-8")
        self.db.put(key, entity_hash.encode("utf-8"))

    def _collect(
        self,
    ) -> Generator[Tuple[str, Optional[str], Optional[str]], None, None]:
        # Use entity_id:version key ordering to set prev_hash if available and
        # compare with curr_hash in the final iteration for that entity_id.
        # More efficient than random access.

        with self.db.iterator(include_key=True, fill_cache=False) as it:
            entity_id: Optional[str] = None
            prev_hash: Optional[str] = None
            curr_hash: Optional[str] = None
            for key, value in it:
                new_id, version_id = key.decode("utf-8").split(":", 1)
                hash = value.decode("utf-8")
                if self.prev is None:
                    yield new_id, None, hash
                    continue
                if new_id != entity_id:
                    if entity_id is not None:
                        yield entity_id, prev_hash, curr_hash
                    entity_id, prev_hash, curr_hash = new_id, None, None
                if self.curr is not None and version_id == self.curr.id:
                    curr_hash = hash
                else:
                    prev_hash = hash
            if entity_id is not None:
                yield entity_id, prev_hash, curr_hash

    def generate(self) -> Generator[Tuple[str, str], None, None]:
        for entity_id, prev_hash, curr_hash in self._collect():
            if prev_hash == curr_hash:
                continue
            if prev_hash is None:
                yield "ADD", entity_id
            elif curr_hash is None:
                yield "DEL", entity_id
            else:
                yield "MOD", entity_id

    def close(self) -> None:
        self.db.close()
        self.fh.close()
