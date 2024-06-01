import json
import shutil
from copy import deepcopy
from typing import Any, Dict
from nomenklatura.versions import Version
from nomenklatura.store import MemoryStore
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver

from zavod import settings
from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.runtime.versions import make_version
from zavod.archive import DELTA_FILE, DATASETS, ARTIFACTS
from zavod.archive import publish_dataset_version
from zavod.exporters import export_dataset
from zavod.exporters.delta import DeltaExporter


ENTITY_A = {"id": "EA", "schema": "Person", "properties": {"name": ["Alice"]}}
ENTITY_B = {"id": "EB", "schema": "Person", "properties": {"name": ["Bob"]}}
ENTITY_C = {"id": "EC", "schema": "Person", "properties": {"name": ["Carl"]}}
ENTITY_CX = {"id": "ECX", "schema": "Person", "properties": {"name": ["Carl Sagan"]}}
ENTITY_D = {"id": "ED", "schema": "Person", "properties": {"name": ["Dory"]}}


def test_delta_exporter(testdataset1: Dataset):
    settings.RUN_VERSION = Version.new()
    testdataset1.exports = {DELTA_FILE}
    archive_path = settings.DATA_PATH / ARTIFACTS
    dataset_path = settings.DATA_PATH / DATASETS / testdataset1.name
    shutil.rmtree(archive_path, ignore_errors=True)
    shutil.rmtree(dataset_path, ignore_errors=True)
    resolver = Resolver[Entity]()

    def e(data: Dict[str, Any]) -> Entity:
        return resolver.apply(Entity.from_data(testdataset1, data))

    store = MemoryStore(testdataset1, resolver)
    writer = store.writer()
    writer.add_entity(e(ENTITY_B))
    writer.add_entity(e(ENTITY_C))
    writer.add_entity(e(ENTITY_CX))
    writer.add_entity(e(ENTITY_D))
    writer.flush()
    view = store.view(testdataset1)
    assert len(list(view.entities())) == 4
    export_dataset(testdataset1, view)
    assert dataset_path.joinpath(DELTA_FILE).exists()
    with open(dataset_path.joinpath(DELTA_FILE), "r") as fh:
        objects = [json.loads(line) for line in fh.readlines()]
        assert len(objects) == 4, objects
        for data in objects:
            assert data["op"] == "ADD"

    make_version(testdataset1.name)
    publish_dataset_version(testdataset1.name)
    # assert False, list(get_redis().keys())
    settings.RUN_VERSION = Version.new()
    store = MemoryStore(testdataset1, resolver)
    writer = store.writer()
    writer.add_entity(e(ENTITY_A))
    writer.add_entity(e(ENTITY_B))
    changed = deepcopy(ENTITY_C)
    changed["properties"] = {"name": ["Charlie"]}
    writer.add_entity(e(changed))
    writer.add_entity(e(ENTITY_CX))
    writer.flush()
    view = store.view(testdataset1)

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
    make_version(testdataset1.name)
    publish_dataset_version(testdataset1.name)
    settings.RUN_VERSION = Version.new()
    canon_id = resolver.decide("EC", "ECX", Judgement.POSITIVE)
    store = MemoryStore(testdataset1, resolver)
    writer = store.writer()
    writer.add_entity(e(ENTITY_A))
    writer.add_entity(e(ENTITY_B))
    changed = deepcopy(ENTITY_C)
    changed["properties"] = {"name": ["Charlie"]}
    writer.add_entity(e(changed))
    writer.add_entity(e(ENTITY_CX))
    writer.flush()
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
    make_version(testdataset1.name)
    publish_dataset_version(testdataset1.name)
    assert len(redis.keys()) > 0
    DeltaExporter.cleanup(testdataset1.name, keep=10)
    assert len(redis.keys()) > 0
    DeltaExporter.cleanup(testdataset1.name, keep=0)
    assert len(redis.keys()) == 0, redis.keys()
