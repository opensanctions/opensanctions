from banal import hash_data
from typing import Any, Generator, Optional
from nomenklatura.kv import get_redis, close_redis, b, bv


from zavod import settings
from zavod.entity import Entity
from zavod.archive import iter_dataset_versions_desc
from zavod.exporters.common import Exporter
from zavod.util import write_json


class DeltaExporter(Exporter):
    TITLE = "Delta files"
    FILE_NAME = "entities.delta.json"
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
        prev_hashes = f"delta:hash:{self.dataset.name}:{previous}"
        prev_entities = f"delta:ents:{self.dataset.name}:{previous}"
        changed_hashes = self.redis.sdiff([self.hashes, prev_hashes])
        for hash in changed_hashes:
            b_entity_id, _ = bv(hash).split(b":", 1)
            entity_id = b_entity_id.decode("utf-8")
            is_curr = self.redis.sismember(self.entities, b_entity_id)
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

    def finish(self) -> None:
        version: Optional[str] = None

        # FIXME: this is a bit of a hack, but we need to find the last
        # version that has a delta state in the redis store.
        for v in iter_dataset_versions_desc(self.dataset.name):
            if self.redis.exists(f"delta:ents:{self.dataset.name}:{v.id}"):
                version = v.id
                break

        with open(self.path, "wb") as fh:
            for op in self.generate(version):
                write_json(op, fh)
        super().finish()
        close_redis()
