from typing import Any, Dict, Set, cast
from functools import cached_property
from nomenklatura.dataset import DataCatalog

from opensanctions.core.dataset import Dataset
from opensanctions.core.source import Source
from opensanctions.core.external import External


class Collection(Dataset):
    """A grouping of individual data sources. Data sources are bundled in order
    to be more useful for list use."""

    TYPE = "collection"

    def __init__(self, catalog: DataCatalog, config: Dict[str, Any]):
        super().__init__(catalog, self.TYPE, config)

    @cached_property
    def sources(self) -> Set[Source]:
        return set([cast(Source, t) for t in self.datasets if t.TYPE == Source.TYPE])

    @cached_property
    def scopes(self) -> Set[Dataset]:
        return set([t for t in self.datasets if t.TYPE in (Source.TYPE, External.TYPE)])

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["sources"] = [s.name for s in self.sources]
        data["scopes"] = [s.name for s in self.scopes]
        data["externals"] = [s.name for s in self.datasets if s.TYPE == External.TYPE]
        return data
