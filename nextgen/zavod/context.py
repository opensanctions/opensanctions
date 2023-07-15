import json
from pathlib import Path
from typing import Any, Generic, Optional, Type, Union

from followthemoney.schema import Schema
from followthemoney.util import make_entity_id
from nomenklatura.entity import CE

from zavod.audit import inspect
from zavod.meta import Dataset
from zavod.archive import PathLike, dataset_resource_path, dataset_path
from zavod.http import fetch_file, make_session
from zavod.logs import get_logger
from zavod.sinks.common import Sink
from zavod.util import join_slug


class GenericZavod(Generic[CE]):
    def __init__(
        self, dataset: Dataset, entity_type: Type[CE], sink: Optional[Sink[CE]] = None
    ):
        self.dataset = dataset
        self.entity_type = entity_type
        self.sink = sink
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

    def make(self, schema: Union[str, Schema]) -> CE:
        """Make a new entity with some dataset context set."""
        return self.entity_type(self.dataset, {"schema": schema})

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

    def inspect(self, obj: Any) -> None:
        """Display an object in a form suitable for inspection."""
        text = inspect(obj)
        if text is not None:
            self.log.info(text)

    def emit(self, entity: CE) -> None:
        if self.sink is None:
            return None
        entity.datasets.add(self.dataset.name)
        return self.sink.emit(entity)

    def close(self) -> None:
        """Flush and tear down the context."""
        self.http.close()
        if self.sink is not None:
            self.sink.close()
