from functools import cache
from nomenklatura.cache import Cache
from nomenklatura.enrich import Enricher, get_enricher

from opensanctions.core.dataset import Dataset


class External(Dataset):
    """A an external data source which is only included in parts via data enrichment."""

    TYPE = "external"

    def __init__(self, catalog, config):
        super().__init__(catalog, self.TYPE, config)
        self.disabled = config.get("disabled", False)
        self.export = False
        self.enricher_config = config.get("config", {})
        assert self.publisher is not None, "No publisher information!"
        # self.publisher = DataPublisher(config.get("publisher", {}))

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
                "publisher": self.publisher.to_dict(),
                "collections": [p.name for p in self.parents],
            }
        )
        return data
