import json
from copy import deepcopy
from typing import Any, Dict
from nomenklatura.kv import get_redis
from nomenklatura.versions import Version
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver

from zavod import settings
from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.runtime.versions import make_version
from zavod.archive import DELTA_FILE, DATASETS
from zavod.archive import publish_dataset_version
from zavod.store import get_store
from zavod.exporters import export_dataset
from zavod.exporters.delta import DeltaExporter


ENTITY_A = {"id": "EA", "schema": "Person", "properties": {"name": ["Alice"]}}
ENTITY_B = {"id": "EB", "schema": "Person", "properties": {"name": ["Bob"]}}
ENTITY_C = {"id": "EC", "schema": "Person", "properties": {"name": ["Carl"]}}
ENTITY_CX = {"id": "ECX", "schema": "Person", "properties": {"name": ["Carl Sagan"]}}
ENTITY_D = {"id": "ED", "schema": "Person", "properties": {"name": ["Dory"]}}


def test_delta_exporter(testdataset1: Dataset):
    redis = get_redis()
    assert len(redis.keys()) == 0
    testdataset1.exports = {DELTA_FILE}
    dataset_path = settings.DATA_PATH / DATASETS / testdataset1.name
    resolver = Resolver[Entity]()
    store = get_store(testdataset1, resolver)

    def e(data: Dict[str, Any]) -> Entity:
        return resolver.apply(Entity.from_data(testdataset1, data))

    version = Version.new("aaa")
    make_version(testdataset1, version, overwrite=True)
    writer = store.writer(version=version.id)
    writer.add_entity(e(ENTITY_B))
    writer.add_entity(e(ENTITY_C))
    writer.add_entity(e(ENTITY_CX))
    writer.add_entity(e(ENTITY_D))
    writer.flush()
    writer.release()
    view = store.view(testdataset1)
    assert len(list(view.entities())) == 4
    export_dataset(testdataset1, view)
    assert dataset_path.joinpath(DELTA_FILE).exists()
    with open(dataset_path.joinpath(DELTA_FILE), "r") as fh:
        objects = [json.loads(line) for line in fh.readlines()]
        assert len(objects) == 4, objects
        for data in objects:
            assert data["op"] == "ADD"

    publish_dataset_version(testdataset1.name)
    # assert False, list(get_redis().keys())

    version2 = Version.new("bbb")
    make_version(testdataset1, version2, overwrite=True)
    writer = store.writer(version=version2.id)
    writer.add_entity(e(ENTITY_A))
    writer.add_entity(e(ENTITY_B))
    changed = deepcopy(ENTITY_C)
    changed["properties"] = {"name": ["Charlie"]}
    writer.add_entity(e(changed))
    writer.add_entity(e(ENTITY_CX))
    writer.flush()
    writer.release()
    assert len(store.get_history(testdataset1.name)) == 2
    view = store.view(testdataset1)
    assert view.vers[0][1] == version2.id

    export_dataset(testdataset1, view)
    assert dataset_path.joinpath(DELTA_FILE).exists()
    with open(dataset_path.joinpath(DELTA_FILE), "r") as fh:
        objects = [json.loads(line) for line in fh.readlines()]
        assert len(objects) == 3, [o["entity"]["id"] for o in objects]
        for data in objects:
            if data["entity"]["id"] == "EC":
                assert data["op"] == "MOD"
            if data["entity"]["id"] == "EA":
                assert data["op"] == "ADD"
            if data["entity"]["id"] == "ED":
                assert data["op"] == "DEL"

    # Round 3: check that the delta exporter can handle resolver changes
    publish_dataset_version(testdataset1.name)
    version3 = Version.new("ccc")
    make_version(testdataset1, version3, overwrite=True)
    canon_id = resolver.decide("EC", "ECX", Judgement.POSITIVE)
    writer = store.writer(version=version3.id)
    writer.add_entity(e(ENTITY_A))
    writer.add_entity(e(ENTITY_B))
    changed = deepcopy(ENTITY_C)
    changed["properties"] = {"name": ["Charlie"]}
    writer.add_entity(e(changed))
    writer.add_entity(e(ENTITY_CX))
    writer.flush()
    writer.release()
    assert len(store.get_history(testdataset1.name)) == 3
    view = store.view(testdataset1)

    export_dataset(testdataset1, view)
    assert dataset_path.joinpath(DELTA_FILE).exists()
    with open(dataset_path.joinpath(DELTA_FILE), "r") as fh:
        objects = [json.loads(line) for line in fh.readlines()]
        assert len(objects) == 3, [o["entity"]["id"] for o in objects]
        for data in objects:
            if data["entity"]["id"] == canon_id.id:
                assert data["op"] == "ADD"
            if data["entity"]["id"] == "EC":
                assert data["op"] == "DEL"
            if data["entity"]["id"] == "ECX":
                assert data["op"] == "DEL"

    # test the cleanup
    publish_dataset_version(testdataset1.name)
    for v in store.get_history(testdataset1.name):
        store.drop_version(testdataset1.name, v)
    assert len(redis.keys()) > 0
    DeltaExporter.cleanup(testdataset1.name, keep=10)
    assert len(redis.keys()) > 0
    DeltaExporter.cleanup(testdataset1.name, keep=0)
    assert len(redis.keys()) == 0, redis.keys()
