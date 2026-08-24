from followthemoney.dataset import Version
from nomenklatura.resolver import Linker

from zavod import settings
from zavod.archive import MANIFEST_FILE, dataset_artifact_path
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.entity import Entity
from zavod.exporters import export_dataset
from zavod.integration import get_dataset_linker
from zavod.meta import Dataset
from zavod.publish import publish_dataset
from zavod.runtime.lake import build_statements_parquet, dump_statements_pack
from zavod.runtime.manifest import Manifest
from zavod.store import View, get_store


def get_manifest(dataset: Dataset, version: Version | None = None) -> Manifest:
    """Load the run manifest for the dataset, creating the run (artifact
    directory and manifest) on first use of this (dataset, version).

    Idempotent, unlike `Manifest.create` — it never wipes an existing run."""
    version = version or settings.RUN_VERSION
    if not dataset_artifact_path(dataset.name, version, MANIFEST_FILE).is_file():
        return Manifest.create(dataset, version)
    return Manifest.load_artifact(dataset, version)


def make_context(dataset: Dataset, version: Version | None = None) -> Context:
    """A Context whose versioned artifact directory exists, so that closing
    it (which writes issues.json there) works outside a full crawl."""
    get_manifest(dataset, version)
    return Context(dataset, version or settings.RUN_VERSION)


def finish_statements(context: Context) -> None:
    """Seal a manually-emitted context's raw statements file and derive the
    run's parquet and pack artifacts from it, as `crawl_dataset` does after
    the crawl. Use after emitting fixture entities through a bare Context, so
    stores and exports built off the run's manifest can read them."""
    context.finalize_statements()
    build_statements_parquet(context.dataset, context.version)
    dump_statements_pack(context.dataset, context.version)


def get_test_view(
    dataset: Dataset,
    linker: Linker[Entity] | None = None,
    version: Version | None = None,
    clear: bool = False,
) -> View:
    """A synced store view over the run pinned by the dataset's manifest."""
    if linker is None:
        linker = get_dataset_linker(dataset)
    manifest = get_manifest(dataset, version)
    store = get_store(manifest, linker)
    store.sync(clear=clear)
    return store.view(dataset)


def run_dataset(
    dataset: Dataset,
    version: Version | None = None,
    linker: Linker[Entity] | None = None,
    publish: bool = True,
) -> Version:
    """The canonical `zavod run`: crawl, sync the store, export and (unless
    publish=False) publish the dataset under the given run version."""
    version = version or settings.RUN_VERSION
    if linker is None:
        linker = get_dataset_linker(dataset)
    crawl_dataset(dataset, version)
    view = get_test_view(dataset, linker=linker, version=version)
    try:
        export_dataset(dataset, version, view)
    finally:
        view.store.close()
    if publish:
        publish_dataset(dataset, version)
    return version
