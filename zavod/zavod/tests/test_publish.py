import json
from typing import Optional
from followthemoney.dataset import VersionHistory
from pydantic import ValidationError
from pytest import raises
from structlog.testing import capture_logs
from logging import Logger

from zavod import settings
from zavod.meta import Dataset
from zavod.archive import DELTA_EXPORT_FILE, get_dataset_artifact, clear_data_path
from zavod.archive import dataset_resource_path
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

STANDARD_EXPORTS = {
    "entities.ftm.json",
    "targets.simple.csv",
    "names.txt",
    "targets.nested.json",
    "senzing.json",
}


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
    artifacts = {str(p.name) for p in artifact_path.glob("*")}
    assert artifacts == {
        # Everything gets archived
        INDEX_FILE,
        ISSUES_FILE,
        ISSUES_LOG,
        VERSIONS_FILE,
        RESOURCES_FILE,
        HASH_FILE,
        DELTA_INDEX_FILE,
        DELTA_EXPORT_FILE,
        STATEMENTS_FILE,
        STATISTICS_FILE,
        "source.csv",
        # Collections-only:
        # CATALOG_FILE,
    } | STANDARD_EXPORTS  # fmt: skip

    # Only index and real resources get published.
    release_artifacts = {str(p.name) for p in release_path.glob("*")}
    assert release_artifacts == {
        INDEX_FILE,
        "source.csv",
    } | STANDARD_EXPORTS  # fmt: skip
    # Nothing's published to 'latest'
    latest_artifacts = {str(p.name) for p in latest_path.glob("*")}
    assert latest_artifacts == set()

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
    artifacts = {str(p.name) for p in artifact_path.glob("*")}
    assert artifacts == {
        # Everything gets archived
        INDEX_FILE,
        ISSUES_FILE,
        VERSIONS_FILE,
        RESOURCES_FILE,
        HASH_FILE,
        DELTA_INDEX_FILE,
        DELTA_EXPORT_FILE,
        STATISTICS_FILE,
        # Collections get a catalog.json
        CATALOG_FILE,
        # Collections don't crawl, so statements.pack is never produced.
        # STATEMENTS_FILE
        # No issue was logged during this export, so the lazily-created
        # issues.log was never written.
        # ISSUES_LOG
    } | STANDARD_EXPORTS  # fmt: skip

    release_artifacts = {str(p.name) for p in release_path.glob("*")}
    # Only index, catalog, and real resources get published.
    # Artifact-only files don't leak into /datasets/.
    assert release_artifacts == {
        INDEX_FILE,
        CATALOG_FILE,
    } | STANDARD_EXPORTS  # fmt: skip
    latest_artifacts = {str(p.name) for p in latest_path.glob("*")}
    assert latest_artifacts == {
        INDEX_FILE,
        CATALOG_FILE,
    } | STANDARD_EXPORTS  # fmt: skip


def test_publish_incomplete_index(testdataset1: Dataset):
    """Publishing is refused when the index is missing run statistics that
    consumers of published datasets rely on, e.g. because no entities were
    emitted (https://github.com/opensanctions/opensanctions/issues/4643)."""
    linker = get_dataset_linker(testdataset1)
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    view = store.view(testdataset1)
    export_dataset(testdataset1, view)

    index_path = dataset_resource_path(testdataset1.name, INDEX_FILE)
    with open(index_path, "r") as fh:
        index = json.load(fh)
    for key in ("entity_count", "target_count", "thing_count", "last_change"):
        del index[key]
    with open(index_path, "w") as fh:
        json.dump(index, fh)

    with raises(ValidationError) as exc_info:
        publish_dataset(testdataset1, republish_to_latest=True)
    errors = {error["loc"][0] for error in exc_info.value.errors()}
    assert errors == {"entity_count", "target_count", "thing_count", "last_change"}

    # Nothing was archived or published:
    published_path = settings.ARCHIVE_PATH / DATASETS
    assert not (published_path / settings.RELEASE / testdataset1.name).exists()
    assert not (published_path / "latest" / testdataset1.name).exists()
    assert _read_history(testdataset1.name) is None


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

    artifacts = {str(p.name) for p in artifact_path.glob("*")}

    # Only very specific files get archived.
    # We want to be really, really sure we'll never backfill from failed runs
    assert artifacts == {
        INDEX_FILE,
        ISSUES_FILE,
        ISSUES_LOG,
        VERSIONS_FILE,
        # We want to be really, really sure we'll never backfill from failed runs
        # so specifically not:
        #
        # STATEMENTS_FILE,
        # RESOURCES_FILE,
        # HASH_FILE,
        # DELTA_INDEX_FILE,
    }  # fmt: skip

    # We don't want failed runs to end up in /datasets
    assert len(list(latest_path.glob("*"))) == 0
    assert len(list(release_path.glob("*"))) == 0


def test_archive_collection_failure(
    testdataset1: Dataset, collection: Dataset, logger: Logger
):
    """Effectively a 'zavod run' on a collection, checking that the the files
    expected to be archived and published are present in the right locations."""
    linker = get_dataset_linker(testdataset1)
    artifacts_path = settings.ARCHIVE_PATH / ARTIFACTS
    published_path = settings.ARCHIVE_PATH / DATASETS
    release_path = published_path / settings.RELEASE / collection.name
    latest_path = published_path / "latest" / collection.name

    # Simulate something that logs results in an issue log during a collection run
    collection.model.exports.add("missing.exp")

    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    view = store.view(testdataset1)
    export_dataset(testdataset1, view)

    export_dataset(collection, view)
    # let's imagine there was an exception causing abort
    archive_failure(collection)

    history = _read_history(collection.name)
    assert history is not None
    assert history.latest is not None
    assert history.latest.id is not None
    artifact_path = artifacts_path / collection.name / history.latest.id

    artifacts = {str(p.name) for p in artifact_path.glob("*")}

    assert artifacts == {
        INDEX_FILE,
        ISSUES_FILE,
        ISSUES_LOG,
        VERSIONS_FILE,
        # We want to be really, really sure we won't see exports from failed runs.
        # Specifically not:
        #
        # STATEMENTS_FILE,
        # RESOURCES_FILE,
        # HASH_FILE,
        # DELTA_INDEX_FILE,
    }  # fmt: skip

    assert len(list(latest_path.glob("*"))) == 0
    assert len(list(release_path.glob("*"))) == 0
