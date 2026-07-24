import mimetypes
from functools import cached_property
from hashlib import sha1
from pathlib import Path
from typing import TYPE_CHECKING, Any

from banal import ensure_dict, ensure_list
from datapatch import Lookup, get_lookups
from followthemoney.dataset import Dataset as FollowTheMoneyDataset
from followthemoney.dataset.coverage import DataCoverage
from followthemoney.dataset.resource import DataResource

from zavod import settings
from zavod.archive import dataset_data_path
from zavod.logs import get_logger
from zavod.meta.assertion import (
    Assertion,
    merge_assertions_config,
    parse_assertions,
)
from zavod.meta.dates import DatesSpec
from zavod.meta.http import HTTP
from zavod.meta.model import DataModel, ZavodDatasetModel
from zavod.meta.names import NamesSpec
from zavod.meta.numbers import NumbersSpec
from zavod.runtime.urls import make_published_url

if TYPE_CHECKING:
    from zavod.meta.catalog import ArchiveBackedCatalog

log = get_logger(__name__)

# Baseline assertions applied to every dataset. They can be overridden
# per-dataset in the `assertions:` YAML block: because the merge happens at the
# config-dict level (see `merge_assertions_config`), a dataset that sets e.g.
# `min.property_fill_rate.Person.name` replaces the default threshold for that
# exact schema/property while keeping the rest. Lower a threshold to 0 to
# effectively disable a `min` default.
#
# `property_fill_rate` only applies to schemata the dataset actually emits: the
# validator skips any schema with zero entities, so naming schemata a dataset
# doesn't produce here is harmless.
DEFAULT_ASSERTIONS: dict[str, Any] = {
    "min": {
        "property_fill_rate": {
            "Person": {"name": 0.95},
            "LegalEntity": {"name": 0.95},
            "Organization": {"name": 0.95},
            "Company": {"name": 0.95},
        }
    }
}


class Dataset(FollowTheMoneyDataset):
    def __init__(self, data: dict[str, Any]):
        super().__init__(data)
        self.model: ZavodDatasetModel = ZavodDatasetModel.model_validate(data)
        self.prefix = self.model.prefix

        # This will make disabled crawlers visible in the metadata:
        if self.model.disabled:
            if self.model.coverage is None:
                self.model.coverage = DataCoverage()
            self.model.coverage.frequency = "never"

        self.config: dict[str, Any] = ensure_dict(data.get("config", {}))
        _inputs = ensure_list(data.get("inputs", []))
        self.inputs: list[str] = [str(x) for x in _inputs]
        """List of other datasets that this dataset depends on as processing inputs."""

        self._data = data
        self.base_path: Path | None = None

        # TODO: this is for backward compatibility, get rid of it one day
        _type = "collection" if self.is_collection else "source"
        self._type: str = data.get("type", _type).lower().strip()

        # The YAML block overrides defaults at the leaf level (see merge_assertions_config).
        user_assertions_config = ensure_dict(data.get("assertions", {}))
        assertions_config = merge_assertions_config(
            DEFAULT_ASSERTIONS, user_assertions_config
        )
        self.assertions: list[Assertion] = list(parse_assertions(assertions_config))
        """
        List of assertions which should be considered warnings if they fail.

        Configured as follows:

        ```yaml
          min:
            schema_entities:
              Person: 160000
              Position: 10000
            country_entities:
              us: 20000
              cn: 8000
            countries: 19
          max:
            schema_entities:
              Person: 180000
        ```
        """

        self.http: HTTP = HTTP(data.get("http", {}))
        """HTTP configuration for this dataset."""

        self.dates: DatesSpec = DatesSpec.model_validate(data.get("dates", {}))
        """Date parsing configuration for this dataset."""

        self.names: NamesSpec = NamesSpec.model_validate(data.get("names", {}))
        """Name cleaning configuration for this dataset."""

        self.numbers: NumbersSpec = NumbersSpec.model_validate(data.get("numbers", {}))
        """Number parsing configuration for this dataset."""

    @cached_property
    def lookups(self) -> dict[str, Lookup]:
        config = self._data.get("lookups", {})
        return get_lookups(config, debug=settings.DEBUG)

    @property
    def url(self) -> str | None:
        return self.model.url

    @property
    def data(self) -> DataModel | None:
        return self.model.data

    def resource_from_path(
        self,
        path: Path,
        mime_type: str | None = None,
        title: str | None = None,
    ) -> "DataResource":
        """Create a resource description object from a local file path."""
        if not path.exists():
            raise ValueError(f"File does not exist: {path}")
        if mime_type is None:
            mime_type, _ = mimetypes.guess_type(path.as_posix(), strict=False)
        dataset_path_ = dataset_data_path(self.name)
        name = path.relative_to(dataset_path_).as_posix()

        digest = sha1()
        size = 0
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(65536)
                if not chunk:
                    break
                size += len(chunk)
                digest.update(chunk)
        checksum = digest.hexdigest()
        return DataResource(
            name=name,
            title=title,
            checksum=checksum,
            mime_type=mime_type,
            size=size,
            url=make_published_url(self.name, name),
        )

    def to_dict(self) -> dict[str, Any]:
        """Generate a metadata export, not including operational details."""
        data = super().to_dict()
        data["hidden"] = self.model.hidden
        data["disabled"] = self.model.disabled
        if not self.model.resolve:
            data["resolve"] = False
        if self.model.full_dataset is not None:
            data["full_dataset"] = self.model.full_dataset
        for resource in data.get("resources", []):
            resource["path"] = resource["name"]
        if self.is_collection:
            # As of mid-2025, the same is now in `children`, but we keep it in `datasets` for backward compatibility.
            data["datasets"] = [d.name for d in self.datasets]
            data["datasets"].remove(self.name)
        else:
            # Should be empty for non-collection datasets, they are dumped from the model as empty lists.
            data.pop("datasets", None)
            data.pop("children", None)
        return data

    def to_opensanctions_dict(self, catalog: "ArchiveBackedCatalog") -> dict[str, Any]:
        """Generate a metadata export in the format expected by the OpenSanctions catalog."""
        data = self.to_dict()
        assert self._type in ("collection", "source", "external"), self._type
        data.pop("resources", None)
        data.pop("version", None)

        # Add the type field: collection, source, or external
        data["type"] = self._type

        # Add the list of collections that this dataset belongs to
        if not self.is_collection:
            collections = [
                p.name for p in catalog.datasets if self in p.datasets and p != self
            ]
            data["collections"] = collections
        if self.model.entry_point is not None:
            data["entry_point"] = self.model.entry_point
        return data
