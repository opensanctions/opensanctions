import json
from typing import Dict, Any, List
from followthemoney.dataset import DataResource

from zavod.meta import Dataset
from zavod.archive import dataset_resource_path
from zavod.archive import RESOURCES_FILE


class DatasetResources(object):
    """Store information about the resources in the dataset that have been emitted
    from the context during runtime."""

    def __init__(self, dataset: Dataset) -> None:
        self.dataset = dataset
        self.path = dataset_resource_path(dataset.name, RESOURCES_FILE)

    def _store_resources(self, resources: List[DataResource]) -> None:
        with open(self.path, "w") as fh:
            objs: List[Dict[str, Any]] = []
            for resource in resources:
                data = resource.model_dump(exclude_none=True)
                data["path"] = resource.name
                objs.append(data)
            json.dump({"resources": objs}, fh, indent=2)

    def save(self, resource: DataResource) -> None:
        resources = [r for r in self.all() if r.name != resource.name]
        resources.append(resource)
        resources = sorted(resources, key=lambda r: r.name)
        self._store_resources(resources)

    def remove(self, name: str) -> None:
        resources = [r for r in self.all() if r.name != name]
        self._store_resources(resources)

    def all(self) -> List[DataResource]:
        resources: List[DataResource] = []
        data: Dict[str, Any] = {}
        # if not self.path.exists():
        #     self.path = get_dataset_artifact(self.dataset.name, RESOURCES_FILE)
        if self.path.exists():
            with open(self.path, "r") as fh:
                data = json.load(fh)
        for raw in data.get("resources", []):
            resources.append(DataResource.model_validate(raw))
        return resources

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()
        with open(self.path, "w") as fh:
            fh.write(json.dumps({"resources": []}))
