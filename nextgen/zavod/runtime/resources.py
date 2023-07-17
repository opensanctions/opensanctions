import json
from typing import Dict, Any, List

from zavod.meta import Dataset, DataResource
from zavod.archive import dataset_resource_path, RESOURCES_FILE


class DatasetResources(object):
    """Store information about the resources in the dataset that have been emitted
    from the context during runtime."""

    def __init__(self, dataset: Dataset) -> None:
        self.path = dataset_resource_path(dataset.name, RESOURCES_FILE)
        self.data: Dict[str, Any] = {}
        if self.path.exists():
            with open(self.path, "r") as fh:
                self.data = json.load(fh)

    def save(self, resource: DataResource) -> None:
        resources = [r for r in self.all() if r.name != resource.name]
        resources.append(resource)
        resources = sorted(resources, key=lambda r: r.name)
        with open(self.path, "w") as fh:
            objs = [r.to_opensanctions_dict() for r in resources]
            json.dump({"resources": objs}, fh, indent=2)

    def all(self) -> List[DataResource]:
        resources: List[DataResource] = []
        data: Dict[str, Any] = {}
        if self.path.exists():
            with open(self.path, "r") as fh:
                data = json.load(fh)
        for raw in data.get("resources", []):
            resources.append(DataResource(raw))
        return resources

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()
