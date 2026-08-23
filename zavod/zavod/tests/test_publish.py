import json

import pytest
from followthemoney.dataset import Version, VersionHistory
from structlog.testing import capture_logs
from logging import Logger

from zavod import settings
from zavod.meta import Dataset
from zavod.archive import DELTA_EXPORT_FILE, backfill_artifact, clear_data_path
from zavod.archive import dataset_artifact_path, get_best_version, stream_statements
from zavod.archive import STATISTICS_FILE, INDEX_FILE, STATEMENTS_FILE
from zavod.archive import DATASETS, ARTIFACTS, VERSIONS_FILE, MANIFEST_FILE
from zavod.archive import ISSUES_FILE, ISSUES_LOG, RESOURCES_FILE
from zavod.archive import HASH_FILE, DELTA_INDEX_FILE, CATALOG_FILE
from zavod.crawl import crawl_dataset
from zavod.store import get_store
from zavod.exporters import export_dataset
from zavod.exporters.metadata import get_catalog_dataset
from zavod.integration import get_dataset_linker
from zavod.publish import publish_dataset, archive_failure
from zavod.runtime.manifest import Manifest
from zavod.exc import RunFailedException
from zavod.tests.util import get_manifest, get_test_view, run_dataset

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


def test_publish_dataset(
    testdataset1: Dataset,
    monkeypatch: pytest.MonkeyPatch,
    # including fixture configures logging, which routes the crawler's test
    # warning into issues.log — part of the expected artifacts below.
    logger: Logger,
):
    """Effectively a 'zavod run' on a dataset.

    Checking that the files expected to be archived are present and that both the
    legacy date-stamped and the latest /datasets/ URLs get their CDN cache purged.
    """

    purged: list[str] = []
    monkeypatch.setattr("zavod.archive.invalidate_archive_cache", purged.append)

    history = _read_history(testdataset1.name)
    assert history is None
    version = run_dataset(testdataset1, publish=False)

    with capture_logs() as cap_logs:
        publish_dataset(testdataset1, version)
    assert not filter_logs(cap_logs, ("warning", "error")), cap_logs
    history = _read_history(testdataset1.name)
    assert history is not None
    assert history.latest == version
    artifact_path = settings.ARCHIVE_PATH / ARTIFACTS / testdataset1.name / version.id
    artifacts = {str(p.name) for p in artifact_path.glob("*")}
    assert artifacts == {
        # Everything in the artifact directory gets archived
        INDEX_FILE,
        ISSUES_FILE,
        ISSUES_LOG,
        VERSIONS_FILE,
        MANIFEST_FILE,
        RESOURCES_FILE,
        HASH_FILE,
        DELTA_INDEX_FILE,
        DELTA_EXPORT_FILE,
        STATEMENTS_FILE,
        STATISTICS_FILE,
        # Registered resources outside the artifact directory:
        "source.csv",
        # Collections-only:
        # CATALOG_FILE,
    } | STANDARD_EXPORTS  # fmt: skip

    release = version.dt.strftime("%Y%m%d")
    assert purged == [
        f"{ARTIFACTS}/{testdataset1.name}/{VERSIONS_FILE}",
        f"{DATASETS}/{release}/{testdataset1.name}/*",
        f"{DATASETS}/latest/{testdataset1.name}/*",
    ]

    artifact_index = artifact_path.joinpath(INDEX_FILE).read_bytes()

    # URLs in the index.json point at the canonical artifacts/{dataset}/{vsn}/ path.
    index = json.loads(artifact_index)
    expected_prefix = (
        f"{settings.ARCHIVE_SITE}/{ARTIFACTS}/{testdataset1.name}/{version.id}/"
    )
    assert index["index_url"] == expected_prefix + INDEX_FILE
    assert len(index["resources"]) > 0
    for resource in index["resources"]:
        assert resource["url"].startswith(expected_prefix), resource
        assert resource["url"].endswith(resource["name"]), resource

    # Test backfill on a clean data path, as in a fresh container:
    clear_data_path(testdataset1.name)
    assert len(list(stream_statements(testdataset1.name, version))) > 5
    # A manifest resolved without local artifacts pins the archived version:
    manifest = Manifest.get_transient(testdataset1)
    assert manifest.datasets[testdataset1.name] == version
    assert len(list(manifest.statements())) > 5
    assert not dataset_artifact_path(testdataset1.name, version, INDEX_FILE).exists()
    path = backfill_artifact(testdataset1.name, version, INDEX_FILE)
    assert path is not None
    assert path.exists()


def test_publish_collection(testdataset1: Dataset, collection: Dataset):
    """Effectively a 'zavod run' on a collection, checking that the files
    expected to be archived are present in the right locations."""
    linker = get_dataset_linker(testdataset1)
    version = settings.RUN_VERSION

    crawl_dataset(testdataset1, version)
    view = get_test_view(testdataset1, linker=linker)
    export_dataset(testdataset1, version, view)

    get_manifest(collection, version)
    export_dataset(collection, version, view)
    view.store.close()
    with capture_logs() as cap_logs:
        publish_dataset(collection, version)
    assert not filter_logs(cap_logs, ("warning", "error")), cap_logs

    history = _read_history(collection.name)
    assert history is not None
    assert history.latest == version
    artifact_path = settings.ARCHIVE_PATH / ARTIFACTS / collection.name / version.id
    artifacts = {str(p.name) for p in artifact_path.glob("*")}
    assert artifacts == {
        # Everything gets archived
        INDEX_FILE,
        ISSUES_FILE,
        VERSIONS_FILE,
        MANIFEST_FILE,
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


def test_empty_crawl_does_not_resurrect_archived_statements(testdataset1: Dataset):
    """A crawl that completes without emitting anything must yield an empty
    store view, not fall back to streaming the previous successful version's
    statements from the archive."""
    linker = get_dataset_linker(testdataset1)
    run_dataset(testdataset1, linker=linker)

    # Run an empty crawl under a fresh version on a clean data path, as in a
    # production `zavod run`:
    clear_data_path(testdataset1.name)
    empty_version = Version.new()
    assert testdataset1.data is not None
    testdataset1.data.format = "EMPTY"
    stats = crawl_dataset(testdataset1, empty_version)
    assert stats.statements == 0

    # The archive holds the previous version's statements, but the empty local
    # statements file from this run takes precedence:
    path = dataset_artifact_path(testdataset1.name, empty_version, STATEMENTS_FILE)
    assert path.is_file()
    manifest = Manifest.load_artifact(testdataset1, empty_version)
    assert len(list(manifest.statements())) == 0

    store = get_store(manifest, linker)
    store.sync(clear=True)
    view = store.view(testdataset1, external=False)
    assert len(list(view.entities())) == 0
    store.close()


def test_failed_run_does_not_replace_latest_metadata(testdataset1: Dataset):
    """A run failing after a successful run archives an index which lists no
    resources. Everything reading the dataset's current metadata - the catalog
    above all - has to keep answering with the last successful run.

    https://github.com/opensanctions/operations/issues/2762
    """
    good_version = run_dataset(testdataset1)

    # A later run fails while crawling, as in a production `zavod run`:
    clear_data_path(testdataset1.name)
    failed_version = Version.new()
    assert failed_version.id != good_version.id
    assert testdataset1.data is not None
    testdataset1.data.format = "FAIL"
    with pytest.raises(RunFailedException):
        crawl_dataset(testdataset1, failed_version)
    archive_failure(testdataset1, failed_version)

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
    # skips the failed run and lands on the last successful one:
    clear_data_path(testdataset1.name)
    best = get_best_version(testdataset1.name)
    assert best == good_version
    path = backfill_artifact(testdataset1.name, best, INDEX_FILE)
    assert path is not None
    with open(path) as fh:
        index = json.load(fh)
    assert index["version"] == good_version.id
    assert index["result"] == "success"
    assert len(index["resources"]) > 0

    catalog_dataset = get_catalog_dataset(testdataset1)
    assert catalog_dataset["version"] == good_version.id
    assert catalog_dataset["last_change"] == index["last_change"]
    assert {r["name"] for r in catalog_dataset["resources"]} >= STANDARD_EXPORTS


def test_archive_failure(testdataset1: Dataset, logger: Logger):
    """Effectively a 'zavod run' on a dataset which fails during the crawl stage,
    checking that the very specific files we want archived are archived."""
    version = settings.RUN_VERSION
    assert testdataset1.data is not None
    testdataset1.data.format = "FAIL"
    try:
        crawl_dataset(testdataset1, version)
    except RunFailedException:
        with capture_logs() as cap_logs:
            archive_failure(testdataset1, version)
        assert not filter_logs(cap_logs, ("warning", "error")), cap_logs
    clear_data_path(testdataset1.name)

    history = _read_history(testdataset1.name)
    assert history is not None
    assert history.latest == version
    artifact_path = settings.ARCHIVE_PATH / ARTIFACTS / testdataset1.name / version.id

    artifacts = {str(p.name) for p in artifact_path.glob("*")}

    # Only very specific files get archived.
    # We want to be really, really sure we'll never backfill from failed runs
    assert artifacts == {
        INDEX_FILE,
        ISSUES_FILE,
        ISSUES_LOG,
        VERSIONS_FILE,
        MANIFEST_FILE,
        # We want to be really, really sure we'll never backfill from failed runs
        # so specifically not:
        #
        # STATEMENTS_FILE,
        # RESOURCES_FILE,
        # HASH_FILE,
        # DELTA_INDEX_FILE,
    }  # fmt: skip


def test_archive_collection_failure(
    testdataset1: Dataset,
    collection: Dataset,
    # including fixture configures logging which is part of this test.
    logger: Logger,
):
    """Effectively a 'zavod run' on a collection, checking that the the files
    expected to be archived are present in the right locations."""
    linker = get_dataset_linker(testdataset1)
    version = settings.RUN_VERSION

    # Simulate something that logs results in an issue log during a collection run.
    collection.model.exports.add("missing.exp")

    crawl_dataset(testdataset1, version)
    view = get_test_view(testdataset1, linker=linker)
    export_dataset(testdataset1, version, view)

    get_manifest(collection, version)
    export_dataset(collection, version, view)
    view.store.close()
    # let's imagine there was an exception causing abort
    archive_failure(collection, version)

    history = _read_history(collection.name)
    assert history is not None
    assert history.latest == version
    artifact_path = settings.ARCHIVE_PATH / ARTIFACTS / collection.name / version.id

    artifacts = {str(p.name) for p in artifact_path.glob("*")}

    assert artifacts == {
        INDEX_FILE,
        ISSUES_FILE,
        ISSUES_LOG,
        VERSIONS_FILE,
        MANIFEST_FILE,
        # We want to be really, really sure we won't see exports from failed runs.
        # Specifically not:
        #
        # STATEMENTS_FILE,
        # RESOURCES_FILE,
        # HASH_FILE,
        # DELTA_INDEX_FILE,
    }  # fmt: skip
