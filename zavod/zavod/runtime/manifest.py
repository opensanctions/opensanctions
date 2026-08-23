import json
import hashlib
from pathlib import Path

from followthemoney.dataset import Version

from zavod import settings
from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.archive import MANIFEST_FILE, STATEMENTS_FILE, StatementGen
from zavod.archive import create_artifact_path, dataset_artifact_path
from zavod.archive import dataset_state_path
from zavod.archive import get_last_successful_version, latest_local_artifact_version
from zavod.archive import iter_statements_path, stream_statements

log = get_logger(__name__)


class Manifest:
    """The pinned composition of a dataset run: which version of each leaf
    dataset the run consumes.

    Version resolution used to happen lazily inside the store builder, so the
    inputs of a collection export were whatever the archive looked like at sync
    time - unrecorded and unrepeatable. A manifest resolves the versions once,
    when the run starts, and everything downstream (store, export, load-db)
    reads exactly the pinned set. It is archived as `manifest.json` next to the
    other run artifacts, so the composition of any published version can be
    inspected later."""

    def __init__(
        self, dataset_name: str, version: Version, datasets: dict[str, Version]
    ) -> None:
        self.dataset_name = dataset_name
        self.version = version
        self.datasets = datasets

    def digest(self) -> str:
        """A stable identifier for the pinned dataset versions.

        Covers only the `datasets` mapping, never the `self` section: a
        transient manifest carries a per-process version there, and store
        reuse across consecutive commands depends on this digest being
        stable for the same pinned content."""
        data = {name: version.id for name, version in sorted(self.datasets.items())}
        text = json.dumps(data, sort_keys=True)
        return hashlib.sha1(text.encode("utf-8")).hexdigest()

    def to_json(self) -> str:
        data = {
            "self": {"name": self.dataset_name, "version": self.version.id},
            "datasets": {
                name: version.id for name, version in sorted(self.datasets.items())
            },
        }
        return json.dumps(data, indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, text: str) -> "Manifest":
        data = json.loads(text)
        self_data = data["self"]
        datasets = {
            name: Version.from_string(id) for name, id in data["datasets"].items()
        }
        version = Version.from_string(self_data["version"])
        return cls(self_data["name"], version, datasets)

    def save(self, path: Path) -> None:
        with open(path, "w") as fh:
            fh.write(self.to_json())

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        with open(path) as fh:
            return cls.from_json(fh.read())

    @classmethod
    def resolve(cls, dataset: Dataset, version: Version | None = None) -> "Manifest":
        """Pin a version for every leaf of the given scope.

        Args:
            dataset: The scope whose leaves are pinned.
            version: The version being produced by the current run. It is
                assigned to the dataset itself; pass None when the scope is
                not being produced (analytical runs), in which case every
                leaf resolves to its best available version.

        Each leaf resolves to the newest local artifact directory holding a
        statements file, then to the last successful version in the archive.
        A leaf with neither is omitted with a warning: the gap is visible in
        the manifest instead of surfacing as silently missing data later."""
        datasets: dict[str, Version] = {}
        for leaf in dataset.leaves:
            if version is not None and leaf.name == dataset.name:
                datasets[leaf.name] = version
                continue
            local = latest_local_artifact_version(leaf.name, STATEMENTS_FILE)
            if local is not None:
                datasets[leaf.name] = local
                continue
            last = get_last_successful_version(leaf.name)
            if last is not None:
                datasets[leaf.name] = last
                continue
            log.warning(
                "No version available for dataset, omitting from manifest",
                dataset=leaf.name,
                scope=dataset.name,
            )
        return cls(dataset.name, version or settings.RUN_VERSION, datasets)

    @classmethod
    def create(cls, dataset: Dataset, version: Version) -> "Manifest":
        """Start a run: create the versioned artifact directory and write the
        manifest that pins the run's inputs.

        Called by the producer of a run only - the crawl for a source dataset,
        the export for a collection. Consumers load the manifest instead."""
        create_artifact_path(dataset.name, version)
        manifest = cls.resolve(dataset, version)
        manifest.save(dataset_artifact_path(dataset.name, version, MANIFEST_FILE))
        return manifest

    @classmethod
    def load_run(cls, dataset: Dataset, version: Version) -> "Manifest":
        """Load the manifest created for a run of the given dataset."""
        return cls.load(dataset_artifact_path(dataset.name, version, MANIFEST_FILE))

    @classmethod
    def get_transient(cls, scope: Dataset, refresh: bool = False) -> "Manifest":
        """Get a pinned scope for analytical runs (xref, dedupe, enrichment).

        The manifest persists under the scope's state directory so that
        consecutive commands operate on the same set of dataset versions;
        pass refresh=True to re-resolve against the current archive state."""
        path = dataset_state_path(scope.name) / MANIFEST_FILE
        if refresh:
            path.unlink(missing_ok=True)
        if path.is_file():
            return cls.load(path)
        manifest = cls.resolve(scope, None)
        manifest.save(path)
        return manifest

    def statements(
        self, external: bool = True, dataset: str | None = None
    ) -> StatementGen:
        """Yield the statements of the pinned dataset versions, from the local
        artifact directory where present, otherwise streamed from the archive.

        Args:
            external: Include statements that are enrichment candidates.
            dataset: Restrict to a single pinned dataset."""
        names = list(self.datasets.keys()) if dataset is None else [dataset]
        for name in names:
            version = self.datasets.get(name)
            if version is None:
                log.warning(
                    "Dataset is not pinned in the manifest",
                    dataset=name,
                    scope=self.dataset_name,
                )
                continue
            path = dataset_artifact_path(name, version, STATEMENTS_FILE)
            if path.is_file():
                yield from iter_statements_path(path, external=external)
                continue
            yield from stream_statements(name, version, external=external)
