import json
from typing import Optional

import pytest
from followthemoney.dataset import Version, VersionHistory
from structlog.testing import capture_logs

from zavod import settings
from zavod.meta import Dataset
from zavod.archive import DELTA_EXPORT_FILE, get_dataset_artifact, clear_data_path
from zavod.archive import iter_dataset_statements, iter_previous_statements
from zavod.archive import STATISTICS_FILE, INDEX_FILE, STATEMENTS_FILE
from zavod.archive import DATASETS, ARTIFACTS, VERSIONS_FILE
from zavod.archive import ISSUES_FILE, ISSUES_LOG, RESOURCES_FILE
from zavod.archive import HASH_FILE, DELTA_INDEX_FILE, CATALOG_FILE
from zavod.crawl import crawl_dataset
from zavod.store import get_store
from zavod.exporters import export_dataset
from zavod.integration import get_dataset_linker
from zavod.publish import publish_dataset, archive_failure
from zavod.exc import RunFailedException


def _read_history(dataset_name: str) -> Optional[VersionHistory]:
    fn = settings.ARCHIVE_PATH / ARTIFACTS / dataset_name / VERSIONS_FILE
    if not fn.exists():
        return None
    with open(fn, "r") as fh:
        return VersionHistory.from_json(fh.read())


def filter_logs(cap_logs: list[dict], levels: tuple[str, ...]) -> list[dict]:
    return [log for log in cap_logs if log.get("log_level") in levels]


def test_publish_dataset(testdataset1: Dataset):
    """Effectively a 'zavod run' on a dataset, first without --latest, then with.

    Checking that the the files expected to be archived and published are present
    in the right locations in each case."""

    linker = get_dataset_linker(testdataset1)
    artifacts_path = settings.ARCHIVE_PATH / ARTIFACTS
    published_path = settings.ARCHIVE_PATH / DATASETS
    release_path = published_path / settings.RELEASE / testdataset1.name
    latest_path = published_path / "latest" / testdataset1.name
    assert not release_path.joinpath(INDEX_FILE).exists()
    assert not latest_path.joinpath(INDEX_FILE).exists()
    history = _read_history(testdataset1.name)
    assert history is None
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    view = store.view(testdataset1)
    export_dataset(testdataset1, view)

    with capture_logs() as cap_logs:
        publish_dataset(testdataset1, republish_to_latest=False)
    assert not filter_logs(cap_logs, ("warning", "error")), cap_logs
    history = _read_history(testdataset1.name)
    assert history is not None
    assert history.latest is not None
    assert history.latest.id is not None
    artifact_path = artifacts_path / testdataset1.name / history.latest.id
    assert artifact_path.exists()
    # Everything gets archived
    assert artifact_path.joinpath(INDEX_FILE).exists()
    assert artifact_path.joinpath(ISSUES_FILE).exists()
    assert artifact_path.joinpath(ISSUES_LOG).exists()
    assert artifact_path.joinpath(VERSIONS_FILE).exists()
    assert artifact_path.joinpath(RESOURCES_FILE).exists()
    assert artifact_path.joinpath(HASH_FILE).exists()
    assert artifact_path.joinpath(DELTA_INDEX_FILE).exists()
    assert artifact_path.joinpath(STATEMENTS_FILE).exists()
    assert artifact_path.joinpath(STATISTICS_FILE).exists()
    assert artifact_path.joinpath("entities.ftm.json").exists()
    assert artifact_path.joinpath(DELTA_EXPORT_FILE).exists()
    # Collections-only:
    assert not artifact_path.joinpath(CATALOG_FILE).exists()

    # Only index and real resources get published.
    assert release_path.joinpath(INDEX_FILE).exists()
    assert release_path.joinpath("entities.ftm.json").exists()
    assert not release_path.joinpath(STATEMENTS_FILE).exists()
    assert not release_path.joinpath(STATISTICS_FILE).exists()
    assert not release_path.joinpath(ISSUES_LOG).exists()
    assert not release_path.joinpath(VERSIONS_FILE).exists()
    assert not release_path.joinpath(RESOURCES_FILE).exists()
    assert not release_path.joinpath(HASH_FILE).exists()
    assert not release_path.joinpath(DELTA_INDEX_FILE).exists()
    assert not release_path.joinpath(DELTA_EXPORT_FILE).exists()
    assert not release_path.joinpath(CATALOG_FILE).exists()

    assert not latest_path.joinpath(INDEX_FILE).exists()

    publish_dataset(testdataset1, republish_to_latest=True)
    assert latest_path.joinpath(INDEX_FILE).exists()

    artifact_index = artifact_path.joinpath(INDEX_FILE).read_bytes()
    assert release_path.joinpath(INDEX_FILE).read_bytes() == artifact_index
    assert latest_path.joinpath(INDEX_FILE).read_bytes() == artifact_index
    artifact_entities = artifact_path.joinpath("entities.ftm.json").read_bytes()
    assert release_path.joinpath("entities.ftm.json").read_bytes() == artifact_entities
    assert latest_path.joinpath("entities.ftm.json").read_bytes() == artifact_entities

    # URLs in the index.json point at the canonical artifacts/{dataset}/{vsn}/ path.
    index = json.loads(artifact_index)
    expected_prefix = (
        f"{settings.ARCHIVE_SITE}/{ARTIFACTS}/{testdataset1.name}/{history.latest.id}/"
    )
    assert index["index_url"] == expected_prefix + INDEX_FILE
    assert len(index["resources"]) > 0
    for resource in index["resources"]:
        assert resource["url"].startswith(expected_prefix), resource
        assert resource["url"].endswith(resource["name"]), resource

    # Test backfill:
    clear_data_path(testdataset1.name)
    assert len(list(iter_dataset_statements(testdataset1))) > 5
    assert len(list(iter_previous_statements(testdataset1))) > 5
    path = get_dataset_artifact(testdataset1.name, INDEX_FILE, backfill=False)
    assert not path.exists()
    path = get_dataset_artifact(testdataset1.name, INDEX_FILE, backfill=True)
    assert path.exists()


def test_publish_collection(testdataset1: Dataset, collection: Dataset):
    """Effectively a 'zavod run' on a collection, checking that the the files
    expected to be archived and published are present in the right locations."""
    linker = get_dataset_linker(testdataset1)
    artifacts_path = settings.ARCHIVE_PATH / ARTIFACTS
    published_path = settings.ARCHIVE_PATH / DATASETS
    release_path = published_path / settings.RELEASE / collection.name
    latest_path = published_path / "latest" / collection.name

    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    view = store.view(testdataset1)
    export_dataset(testdataset1, view)

    export_dataset(collection, view)
    with capture_logs() as cap_logs:
        publish_dataset(collection, republish_to_latest=True)
    assert not filter_logs(cap_logs, ("warning", "error")), cap_logs

    history = _read_history(collection.name)
    assert history is not None
    assert history.latest is not None
    artifact_path = artifacts_path / collection.name / history.latest.id
    assert artifact_path.exists()
    # Everything gets archived
    assert artifact_path.joinpath(INDEX_FILE).exists()
    assert artifact_path.joinpath("entities.ftm.json").exists()
    assert artifact_path.joinpath(ISSUES_FILE).exists()
    assert artifact_path.joinpath(VERSIONS_FILE).exists()
    assert artifact_path.joinpath(RESOURCES_FILE).exists()
    assert artifact_path.joinpath(HASH_FILE).exists()
    assert artifact_path.joinpath(DELTA_INDEX_FILE).exists()
    assert artifact_path.joinpath(STATISTICS_FILE).exists()
    # Collections get a catalog.json
    assert artifact_path.joinpath(CATALOG_FILE).exists()
    # Collections don't crawl, so statements.pack is never produced.
    assert not artifact_path.joinpath(STATEMENTS_FILE).exists()
    # No issue was logged during this export, so the lazily-created
    # issues.log was never written.
    assert not artifact_path.joinpath(ISSUES_LOG).exists()

    # Only index, catalog, and real resources get published.
    assert release_path.joinpath(INDEX_FILE).exists()
    assert release_path.joinpath(CATALOG_FILE).exists()
    assert release_path.joinpath("entities.ftm.json").exists()
    assert latest_path.joinpath(INDEX_FILE).exists()
    assert latest_path.joinpath(CATALOG_FILE).exists()
    assert latest_path.joinpath("entities.ftm.json").exists()
    # Artifact-only files don't leak into /datasets/.
    assert not release_path.joinpath(ISSUES_LOG).exists()
    assert not release_path.joinpath(VERSIONS_FILE).exists()
    assert not release_path.joinpath(HASH_FILE).exists()


def test_archive_failure(testdataset1: Dataset):
    """Effectively a 'zavod run' on a dataset which fails during the crawl stage,
    checking that the very specific files we want archived are archived, and that
    nothing is published to /datasets/."""
    published_path = settings.ARCHIVE_PATH / DATASETS
    artifacts_path = settings.ARCHIVE_PATH / ARTIFACTS
    release_path = published_path / settings.RELEASE / testdataset1.name
    latest_path = published_path / "latest" / testdataset1.name
    assert testdataset1.data is not None
    testdataset1.data.format = "FAIL"
    try:
        crawl_dataset(testdataset1)
    except RunFailedException:
        with capture_logs() as cap_logs:
            archive_failure(testdataset1)
        assert not filter_logs(cap_logs, ("warning", "error")), cap_logs
    clear_data_path(testdataset1.name)

    history = _read_history(testdataset1.name)
    assert history is not None
    assert history.latest is not None
    assert history.latest.id is not None
    artifact_path = artifacts_path / testdataset1.name / history.latest.id

    # Only very specific files get archived.
    assert artifact_path.joinpath(INDEX_FILE).exists()
    assert artifact_path.joinpath(ISSUES_FILE).exists()
    assert artifact_path.joinpath(ISSUES_LOG).exists()
    assert artifact_path.joinpath(VERSIONS_FILE).exists()
    # We want to be really, really sure we'll never backfill from failed runs
    assert not artifact_path.joinpath(STATEMENTS_FILE).exists()
    assert not artifact_path.joinpath(RESOURCES_FILE).exists()
    assert not artifact_path.joinpath(HASH_FILE).exists()
    assert not artifact_path.joinpath(DELTA_INDEX_FILE).exists()
    assert not artifact_path.joinpath(STATISTICS_FILE).exists()

    # We don't want failed runs to end up in /datasets
    assert not latest_path.joinpath(INDEX_FILE).exists()
    assert not latest_path.joinpath(STATEMENTS_FILE).exists()
    assert not latest_path.joinpath(ISSUES_FILE).exists()
    assert len(list(latest_path.glob("*"))) == 0
    assert len(list(release_path.glob("*"))) == 0


def test_archive_failure_no_stale_statistics(
    testdataset1: Dataset, monkeypatch: pytest.MonkeyPatch
):
    """A failed run following a successful one must not resurrect the previous
    run's statistics from the archive in its failure index."""
    linker = get_dataset_linker(testdataset1)
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    export_dataset(testdataset1, store.view(testdataset1))
    publish_dataset(testdataset1, republish_to_latest=True)
    successful_version = settings.RUN_VERSION

    # Fail a subsequent run under a fresh version, on a clean data path as in
    # a production `zavod run`:
    clear_data_path(testdataset1.name)
    monkeypatch.setattr(settings, "RUN_VERSION", Version.new())
    assert testdataset1.data is not None
    testdataset1.data.format = "FAIL"
    with pytest.raises(RunFailedException):
        crawl_dataset(testdataset1)
    archive_failure(testdataset1)

    history = _read_history(testdataset1.name)
    assert history is not None
    assert history.latest is not None
    assert history.latest.id == settings.RUN_VERSION.id
    artifact_path = (
        settings.ARCHIVE_PATH / ARTIFACTS / testdataset1.name / history.latest.id
    )
    # The failed version must not have statistics archived under it...
    assert not artifact_path.joinpath(STATISTICS_FILE).exists()
    # ...and its index must not carry counts from the previous successful run:
    index = json.loads(artifact_path.joinpath(INDEX_FILE).read_text())
    assert index["result"] == "failure"
    assert "entity_count" not in index
    assert "thing_count" not in index
    assert "target_count" not in index

    # The previous successful version's statistics remain untouched:
    successful_path = (
        settings.ARCHIVE_PATH / ARTIFACTS / testdataset1.name / successful_version.id
    )
    assert successful_path.joinpath(STATISTICS_FILE).exists()
    successful_index = json.loads(successful_path.joinpath(INDEX_FILE).read_text())
    assert successful_index["entity_count"] > 0
