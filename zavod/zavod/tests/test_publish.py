import json

import pytest
from followthemoney.dataset import Version, VersionHistory
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
from zavod.exporters.metadata import get_catalog_dataset
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


def _read_history(dataset_name: str) -> VersionHistory | None:
    fn = settings.ARCHIVE_PATH / ARTIFACTS / dataset_name / VERSIONS_FILE
    if not fn.exists():
        return None
    with open(fn) as fh:
        return VersionHistory.from_json(fh.read())


def filter_logs(cap_logs: list[dict], levels: tuple[str, ...]) -> list[dict]:
    return [log for log in cap_logs if log.get("log_level") in levels]


def test_publish_dataset(testdataset1: Dataset, monkeypatch: pytest.MonkeyPatch):
    """Effectively a 'zavod run' on a dataset, first without --latest, then with.

    Checking that the files expected to be archived are present, that nothing
    is copied into /datasets/ (those URLs are served as redirects into
    /artifacts/, operations#2641), and that the right /datasets/ URLs get
    their CDN cache purged in each case."""

    purged: list[str] = []
    monkeypatch.setattr("zavod.archive.invalidate_archive_cache", purged.append)

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

    # Nothing gets copied into /datasets/.
    assert len(list(release_path.glob("*"))) == 0
    assert len(list(latest_path.glob("*"))) == 0

    # Without --latest, only the /datasets/{RELEASE}/ URLs get purged.
    release_index = f"{DATASETS}/{settings.RELEASE}/{testdataset1.name}/{INDEX_FILE}"
    latest_index = f"{DATASETS}/latest/{testdataset1.name}/{INDEX_FILE}"
    assert release_index in purged
    assert latest_index not in purged

    publish_dataset(testdataset1, republish_to_latest=True)
    assert len(list(release_path.glob("*"))) == 0
    assert len(list(latest_path.glob("*"))) == 0
    assert latest_index in purged

    artifact_index = artifact_path.joinpath(INDEX_FILE).read_bytes()

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

    # Nothing gets copied into /datasets/.
    assert len(list(release_path.glob("*"))) == 0
    assert len(list(latest_path.glob("*"))) == 0


def test_empty_crawl_does_not_resurrect_archived_statements(
    testdataset1: Dataset, monkeypatch: pytest.MonkeyPatch
):
    """A crawl that completes without emitting anything must yield an empty
    store view, not fall back to streaming the previous successful version's
    statements from the archive."""
    linker = get_dataset_linker(testdataset1)
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    export_dataset(testdataset1, store.view(testdataset1))
    publish_dataset(testdataset1, republish_to_latest=True)
    store.close()

    # Run an empty crawl under a fresh version on a clean data path, as in a
    # production `zavod run`:
    clear_data_path(testdataset1.name)
    monkeypatch.setattr(settings, "RUN_VERSION", Version.new())
    assert testdataset1.data is not None
    testdataset1.data.format = "EMPTY"
    stats = crawl_dataset(testdataset1)
    assert stats.statements == 0

    # The archive holds the previous version's statements, but the empty local
    # statements file from this run takes precedence:
    assert dataset_resource_path(testdataset1.name, STATEMENTS_FILE).is_file()
    assert len(list(iter_dataset_statements(testdataset1))) == 0

    store = get_store(testdataset1, linker)
    store.sync(clear=True)
    view = store.view(testdataset1, external=False)
    assert len(list(view.entities())) == 0
    store.close()


def test_failed_run_does_not_replace_latest_metadata(
    testdataset1: Dataset, monkeypatch: pytest.MonkeyPatch
):
    """A run failing after a successful one archives an index which lists no
    resources. Everything reading the dataset's current metadata - the catalog
    above all - has to keep answering with the last successful run.

    https://github.com/opensanctions/operations/issues/2762
    """
    linker = get_dataset_linker(testdataset1)
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, linker)
    store.sync()
    export_dataset(testdataset1, store.view(testdataset1))
    publish_dataset(testdataset1, republish_to_latest=True)
    store.close()
    good_version = settings.RUN_VERSION

    # A later run fails while crawling, as in a production `zavod run`:
    clear_data_path(testdataset1.name)
    monkeypatch.setattr(settings, "RUN_VERSION", Version.new())
    failed_version = settings.RUN_VERSION
    assert failed_version.id != good_version.id
    assert testdataset1.data is not None
    testdataset1.data.format = "FAIL"
    with pytest.raises(RunFailedException):
        crawl_dataset(testdataset1)
    archive_failure(testdataset1)

    # The failed run is the newest version of the dataset, and its index has no
    # resources to offer:
    history = _read_history(testdataset1.name)
    assert history is not None
    assert history.latest == failed_version
    assert history.last_successful == good_version
    failed_index = (
        settings.ARCHIVE_PATH
        / ARTIFACTS
        / testdataset1.name
        / failed_version.id
        / INDEX_FILE
    )
    with open(failed_index) as fh:
        assert json.load(fh)["resources"] == []

    # Backfilling the index - as a catalog export in a fresh container does -
    # skips it and lands on the last successful run:
    clear_data_path(testdataset1.name)
    with open(get_dataset_artifact(testdataset1.name, INDEX_FILE)) as fh:
        index = json.load(fh)
    assert index["version"] == good_version.id
    assert index["result"] == "success"
    assert len(index["resources"]) > 0

    catalog_dataset = get_catalog_dataset(testdataset1)
    assert catalog_dataset["version"] == good_version.id
    assert catalog_dataset["last_change"] == index["last_change"]
    assert {r["name"] for r in catalog_dataset["resources"]} >= STANDARD_EXPORTS


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
