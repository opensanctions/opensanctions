import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List

from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver
from nomenklatura.versions import Version
from zavod import settings
from zavod.archive import DELTA_EXPORT_FILE, DATASETS, DELTA_INDEX_FILE
from zavod.entity import Entity
from zavod.exporters import export_dataset
from zavod.meta import Dataset
from zavod.publish import _publish_artifacts
from zavod.runtime.versions import make_version
from zavod.store import get_store

ENTITY_A = {"id": "EA", "schema": "Person", "properties": {"name": ["Alice"]}}
ENTITY_B = {"id": "EB", "schema": "Person", "properties": {"name": ["Bob"]}}
ENTITY_C = {"id": "EC", "schema": "Person", "properties": {"name": ["Carl"]}}
ENTITY_CX = {"id": "ECX", "schema": "Person", "properties": {"name": ["Carl Sagan"]}}
ENTITY_D = {"id": "ED", "schema": "Person", "properties": {"name": ["Dory"]}}


def _assert_delta_index_matches_expected(path: Path, expected_versions: List[str]):
    index = json.loads(path.read_text())

    # Check the "versions" key
    assert len(index["versions"]) == len(expected_versions)
    for version in expected_versions:
        assert version in index["versions"], version
        version_url = index["versions"][version]
        assert version_url.startswith("https://")
        assert f"{version}/{DELTA_EXPORT_FILE}" in version_url

    # Test the "version_list" key, which should be equivalent to "versions"
    assert index["unstable"]["version_list"][0]["url"].startswith(
        "https://data.opensanctions.org/artifacts/"
    )
    version_list_versions = [v["version"] for v in index["unstable"]["version_list"]]
    # All the versions in the dict are also in the list
    assert set(version_list_versions) == set(expected_versions)
    # The list is (reverse) sorted
    assert version_list_versions == sorted(version_list_versions, reverse=True)


def test_delta_exporter(testdataset1: Dataset, resolver: Resolver):
    testdataset1.exports = {DELTA_EXPORT_FILE}
    dataset_path = settings.DATA_PATH / DATASETS / testdataset1.name
    store = get_store(testdataset1, resolver)

    def e(data: Dict[str, Any]) -> Entity:
        return resolver.apply(Entity.from_data(testdataset1, data))

    version = Version.new("aaa")
    make_version(testdataset1, version, overwrite=True)
    store.clear()
    writer = store.writer()
    writer.add_entity(e(ENTITY_B))
    writer.add_entity(e(ENTITY_C))
    writer.add_entity(e(ENTITY_CX))
    writer.add_entity(e(ENTITY_D))
    writer.flush()
    view = store.view(testdataset1)
    assert len(list(view.entities())) == 4
    export_dataset(testdataset1, view)

    assert dataset_path.joinpath(DELTA_EXPORT_FILE).exists()
    with open(dataset_path.joinpath(DELTA_EXPORT_FILE), "r") as fh:
        objects = [json.loads(line) for line in fh.readlines()]
        assert len(objects) == 4, objects
        for data in objects:
            assert data["op"] == "ADD"

    expected_versions = [str(version)]
    _assert_delta_index_matches_expected(
        dataset_path / DELTA_INDEX_FILE, expected_versions
    )

    _publish_artifacts(testdataset1)

    version2 = Version.new("bbb")
    make_version(testdataset1, version2, overwrite=True)
    store.clear()
    writer = store.writer()
    writer.add_entity(e(ENTITY_A))
    writer.add_entity(e(ENTITY_B))
    changed = deepcopy(ENTITY_C)
    changed["properties"] = {"name": ["Charlie"]}
    writer.add_entity(e(changed))
    writer.add_entity(e(ENTITY_CX))
    writer.flush()

    export_dataset(testdataset1, view)
    assert dataset_path.joinpath(DELTA_EXPORT_FILE).exists()
    with open(dataset_path.joinpath(DELTA_EXPORT_FILE), "r") as fh:
        objects = [json.loads(line) for line in fh.readlines()]
        assert len(objects) == 3, [o["entity"]["id"] for o in objects]
        for data in objects:
            if data["entity"]["id"] == "EC":
                assert data["op"] == "MOD"
            if data["entity"]["id"] == "EA":
                assert data["op"] == "ADD"
            if data["entity"]["id"] == "ED":
                assert data["op"] == "DEL"
    expected_versions.append(str(version2))
    _assert_delta_index_matches_expected(
        dataset_path / DELTA_INDEX_FILE, expected_versions
    )

    # Round 3: check that the delta exporter can handle resolver changes
    _publish_artifacts(testdataset1)
    version3 = Version.new("ccc")
    make_version(testdataset1, version3, overwrite=True)
    canon_id = resolver.decide("EC", "ECX", Judgement.POSITIVE)
    store.clear()
    writer = store.writer()
    writer.add_entity(e(ENTITY_A))
    writer.add_entity(e(ENTITY_B))
    changed = deepcopy(ENTITY_C)
    changed["properties"] = {"name": ["Charlie"]}
    writer.add_entity(e(changed))
    writer.add_entity(e(ENTITY_CX))
    writer.flush()
    # writer.release()
    # assert len(store.get_history(testdataset1.name)) == 3
    view = store.view(testdataset1)

    export_dataset(testdataset1, view)
    assert dataset_path.joinpath(DELTA_EXPORT_FILE).exists()
    with open(dataset_path.joinpath(DELTA_EXPORT_FILE), "r") as fh:
        objects = [json.loads(line) for line in fh.readlines()]
        assert len(objects) == 3, [o["entity"]["id"] for o in objects]
        for data in objects:
            if data["entity"]["id"] == canon_id.id:
                assert data["op"] == "ADD"
            if data["entity"]["id"] == "EC":
                assert data["op"] == "DEL"
            if data["entity"]["id"] == "ECX":
                assert data["op"] == "DEL"
    expected_versions.append(str(version3))
    _assert_delta_index_matches_expected(
        dataset_path / DELTA_INDEX_FILE, expected_versions
    )
