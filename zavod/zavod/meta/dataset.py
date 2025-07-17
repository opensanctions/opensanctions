import mimetypes
from hashlib import sha1
from pathlib import Path
from functools import cached_property
from typing import TYPE_CHECKING, Dict, Any, Optional, List

from banal import ensure_list, ensure_dict
from datapatch import get_lookups, Lookup

from followthemoney.dataset import Dataset as FollowTheMoneyDataset
from followthemoney.dataset.coverage import DataCoverage
from followthemoney.dataset.resource import DataResource

from zavod import settings
from zavod.archive import dataset_data_path
from zavod.logs import get_logger
from zavod.meta.assertion import Assertion, parse_assertions, Comparison, Metric
from zavod.meta.model import DataModel, ZavodDatasetModel
from zavod.meta.dates import DatesSpec
from zavod.meta.http import HTTP

if TYPE_CHECKING:
    from zavod.meta.catalog import ArchiveBackedCatalog

log = get_logger(__name__)


class Dataset(FollowTheMoneyDataset):
    def __init__(self, data: Dict[str, Any]):
        super().__init__(data)
        self.model: ZavodDatasetModel = ZavodDatasetModel.model_validate(data)
        self.prefix = self.model.prefix

        # This will make disabled crawlers visible in the metadata:
        if self.model.disabled:
            if self.model.coverage is None:
                self.model.coverage = DataCoverage()
            self.model.coverage.frequency = "never"

        self.config: Dict[str, Any] = ensure_dict(data.get("config", {}))
        _inputs = ensure_list(data.get("inputs", []))
        self.inputs: List[str] = [str(x) for x in _inputs]
        """List of other datasets that this dataset depends on as processing inputs."""

        self._data = data
        self.base_path: Optional[Path] = None

        # TODO: this is for backward compatibility, get rid of it one day
        _type = "collection" if self.is_collection else "source"
        self._type: str = data.get("type", _type).lower().strip()

        self.assertions: List[Assertion] = list(
            parse_assertions(data.get("assertions", {}))
        )
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

        self.assertions.append(
            # At least one entity in dataset
            Assertion(Metric.ENTITY_COUNT, Comparison.GTE, 1, None, None)
        )

        self.http: HTTP = HTTP(data.get("http", {}))
        """HTTP configuration for this dataset."""

        self.dates: DatesSpec = DatesSpec(data.get("dates", {}))
        """Date parsing configuration for this dataset."""

    @cached_property
    def lookups(self) -> Dict[str, Lookup]:
        config = self._data.get("lookups", {})
        return get_lookups(config, debug=settings.DEBUG)

    @property
    def data(self) -> Optional[DataModel]:
        return self.model.data

    def resource_from_path(
        self,
        path: Path,
        mime_type: Optional[str] = None,
        title: Optional[str] = None,
    ) -> "DataResource":
        """Create a resource description object from a local file path."""
        from zavod.runtime.urls import make_published_url

        if not path.exists():
            raise ValueError("File does not exist: %s" % path)
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

    def to_dict(self) -> Dict[str, Any]:
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
            # HACK backwards compatibility
            data["datasets"] = [d.name for d in self.datasets]
            data["datasets"].remove(self.name)
        return data

    def to_opensanctions_dict(self, catalog: "ArchiveBackedCatalog") -> Dict[str, Any]:
        """Generate a backward-compatible metadata export."""
        data = self.to_dict()
        assert self._type in ("collection", "source", "external"), self._type
        data.pop("resources", None)
        data.pop("version", None)
        # data.pop("children", None)
        # data.pop("datasets", None)
        data["type"] = self._type
        if not self.is_collection:
            collections = [
                p.name for p in catalog.datasets if self in p.datasets and p != self
            ]
            data["collections"] = collections
        if self.model.entry_point is not None:
            data["entry_point"] = self.model.entry_point
        return data
