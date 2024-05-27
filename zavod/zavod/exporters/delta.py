from banal import hash_data
from typing import Any, Generator, Optional
from nomenklatura.kv import get_redis, b, bv


from zavod import settings
from zavod.entity import Entity
from zavod.archive import DELTA_FILE, iter_dataset_versions
from zavod.exporters.common import Exporter
from zavod.util import write_json


class DeltaExporter(Exporter):
    TITLE = "Delta files"
    FILE_NAME = DELTA_FILE
    MIME_TYPE = "application/json"

    def setup(self) -> None:
        super().setup()
        self.redis = get_redis()
        self.hashes = f"delta:hash:{self.dataset.name}:{settings.RUN_VERSION}"
        self.entities = f"delta:ents:{self.dataset.name}:{settings.RUN_VERSION}"

    def feed(self, entity: Entity) -> None:
        if entity.id is None:
            return
        self.redis.sadd(self.entities, b(entity.id))
        entity_hash = hash_data((entity.id, entity.schema.name, entity.properties))
        self.redis.sadd(self.hashes, b(f"{entity.id}:{entity_hash}"))

    def generate(self, previous: Optional[str]) -> Generator[Any, None, None]:
        if previous is None:
            # The redis store doesn't offer a delta image, so we're doing
            # a full export here.
            for initial in self.view.entities():
                yield {"op": "ADD", "entity": initial.to_dict()}
            return
        tmp_fwd = f"delta:fwd:{self.dataset.name}"
        tmp_bwd = f"delta:bwd:{self.dataset.name}"
        prev_hashes = f"delta:hash:{self.dataset.name}:{previous}"
        prev_entities = f"delta:ents:{self.dataset.name}:{previous}"
        self.redis.sdiffstore(tmp_fwd, [self.hashes, prev_hashes])
        self.redis.sdiffstore(tmp_bwd, [prev_hashes, self.hashes])
        changed_hashes = self.redis.sunion([tmp_fwd, tmp_bwd])
        prev_id: Optional[str] = None
        for hash in sorted(changed_hashes):
            b_entity_id, _ = bv(hash).split(b":", 1)
            entity_id = b_entity_id.decode("utf-8")
            if entity_id == prev_id:
                continue
            prev_id = entity_id
            is_curr = self.redis.sismember(self.entities, entity_id)
            if not is_curr:
                yield {"op": "DEL", "entity": {"id": entity_id}}
                continue
            entity = self.view.get_entity(entity_id)
            if entity is None:  # wat
                continue
            is_prev = self.redis.sismember(prev_entities, entity_id)
            if not is_prev:
                yield {"op": "ADD", "entity": entity.to_dict()}
                continue

            yield {"op": "MOD", "entity": entity.to_dict()}
        self.redis.delete(tmp_fwd, tmp_bwd)

    def finish(self) -> None:
        version: Optional[str] = None

        # FIXME: this is a bit of a hack, but we need to find the last
        # version that has a delta state in the redis store.
        for v in iter_dataset_versions(self.dataset.name):
            if self.redis.exists(f"delta:ents:{self.dataset.name}:{v.id}"):
                version = v.id
                break

        with open(self.path, "wb") as fh:
            for op in self.generate(version):
                write_json(op, fh)
        super().finish()

    @classmethod
    def cleanup(cls, dataset_name: str, keep: int = 3) -> None:
        redis = get_redis()
        for idx, v in enumerate(iter_dataset_versions(dataset_name)):
            if idx < keep:
                continue
            redis.delete(
                f"delta:ents:{dataset_name}:{v.id}",
                f"delta:hash:{dataset_name}:{v.id}",
                f"delta:fwd:{dataset_name}",
                f"delta:bwd:{dataset_name}",
            )
