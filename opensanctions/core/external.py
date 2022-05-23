import os
from functools import cache
from typing import Set
from followthemoney.types import registry
from nomenklatura.cache import Cache
from nomenklatura.enrich import Enricher, get_enricher

from opensanctions.core.dataset import Dataset


class External(Dataset):
    """A an external data source which is only included in parts via data enrichment."""

    TYPE = "external"

    def __init__(self, file_path, config):
        super().__init__(self.TYPE, file_path, config)
        self.url = config.get("url", "")
        self.disabled = config.get("disabled", False)
        self.enricher_config = config.get("config", {})

    @cache
    def get_enricher(self, cache: Cache) -> Enricher:
        """Load and configure the enricher interface."""
        config = dict(self.enricher_config)
        enricher_type = config.pop("type")
        enricher_cls = get_enricher(enricher_type)
        if enricher_cls is None:
            raise RuntimeError("Could load enricher: %s" % enricher_type)
        return enricher_cls(self, cache, config)

    def to_dict(self):
        data = super().to_dict()
        data.update(
            {
                "url": self.url,
                "disabled": self.disabled,
                "collections": self.collections,
            }
        )
        return data
