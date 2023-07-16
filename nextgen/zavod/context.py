import json
from pathlib import Path
from typing import Any, Optional, Union, Dict, List
from datapatch import LookupException, Result, Lookup
from followthemoney.schema import Schema
from followthemoney.util import make_entity_id

from zavod.audit import inspect
from zavod.meta import Dataset, get_catalog
from zavod.entity import Entity
from zavod.archive import PathLike, dataset_resource_path, dataset_path
from zavod.http import fetch_file, make_session
from zavod.logs import get_logger
from zavod.util import join_slug


class Context(object):
    def __init__(self, dataset: Dataset):
        self.dataset = dataset
        self.log = get_logger(dataset.name)
        self.http = make_session()

    def get_resource_path(self, name: PathLike) -> Path:
        return dataset_resource_path(self.dataset.name, name)

    def export_metadata(self, name: PathLike = "index.json") -> Path:
        path = self.get_resource_path(name)
        with open(path, "w") as fh:
            json.dump(self.dataset.to_dict(), fh)
        return path

    def fetch_resource(
        self,
        name: str,
        url: str,
        auth: Optional[Any] = None,
        headers: Optional[Any] = None,
    ) -> Path:
        """Fetch a URL into a file located in the current run folder,
        if it does not exist."""
        return fetch_file(
            self.http,
            url,
            name,
            data_path=dataset_path(self.dataset.name),
            auth=auth,
            headers=headers,
        )

    def make(self, schema: Union[str, Schema]) -> Entity:
        """Make a new entity with some dataset context set."""
        return Entity(self.dataset, {"schema": schema})

    def make_slug(
        self, *parts: Optional[str], strict: bool = True, prefix: Optional[str] = None
    ) -> Optional[str]:
        prefix = self.dataset.prefix if prefix is None else prefix
        return join_slug(*parts, prefix=prefix, strict=strict)

    def make_id(
        self, *parts: Optional[str], prefix: Optional[str] = None
    ) -> Optional[str]:
        hashed = make_entity_id(*parts, key_prefix=self.dataset.name)
        if hashed is None:
            return None
        return self.make_slug(hashed, prefix=prefix, strict=True)

    def lookup_value(
        self,
        lookup: str,
        value: Optional[str],
        default: Optional[str] = None,
        dataset: Optional[str] = None,
    ) -> Optional[str]:
        try:
            lookup_obj = self.get_lookup(lookup, dataset=dataset)
            return lookup_obj.get_value(value, default=default)
        except LookupException:
            return default

    def get_lookup(self, lookup: str, dataset: Optional[str] = None) -> Lookup:
        ds = get_catalog().require(dataset) if dataset is not None else self.dataset
        return ds.lookups[lookup]

    def lookup(
        self, lookup: str, value: Optional[str], dataset: Optional[str] = None
    ) -> Optional[Result]:
        return self.get_lookup(lookup, dataset=dataset).match(value)

    def inspect(self, obj: Any) -> None:
        """Display an object in a form suitable for inspection."""
        text = inspect(obj)
        if text is not None:
            self.log.info(text)

    def audit_data(
        self, data: Dict[Optional[str], Any], ignore: List[str] = []
    ) -> None:
        """Print the formatted data object if it contains any fields not explicitly
        excluded by the ignore list. This is used to warn about unexpected data in
        the source by removing the fields one by one and then inspecting the rest."""
        cleaned = {}
        for key, value in data.items():
            if key in ignore:
                continue
            if value is None or value == "":
                continue
            cleaned[key] = value
        if len(cleaned):
            self.log.warn("Unexpected data found", data=cleaned)

    def emit(self, entity: Entity, target: bool = False) -> None:
        pass

    def close(self) -> None:
        """Flush and tear down the context."""
        self.http.close()
        pass
