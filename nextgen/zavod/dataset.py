from typing import Dict, Any, TypeVar
from normality import slugify
from nomenklatura.dataset import Dataset, DataCatalog
from nomenklatura.util import datetime_iso

from zavod import settings

ZD = TypeVar("ZD", bound="ZavodDataset")


class ZavodDataset(Dataset):
    def __init__(self, catalog: DataCatalog[ZD], data: Dict[str, Any]):
        super().__init__(catalog, data)
        self.prefix: str = data.get("prefix", slugify(self.name, sep="-"))
        if self.updated_at is None:
            self.updated_at = datetime_iso(settings.RUN_TIME)
