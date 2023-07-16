from typing import Generator
from nomenklatura.enrich import Enricher
from nomenklatura.entity import CE

from zavod.runner import run_dataset


class StubEnricher(Enricher):
    __test__ = False

    def match(self, entity: CE) -> Generator[CE, None, None]:
        if entity.schema.name != "Person":
            return
        result = self.make_entity(entity, "Person")
        result.id = f"enrich-{entity.id}"
        result.add("name", entity.get("name"))
        result.add("birthDate", entity.get("birthDate"))
        result.add("sourceUrl", "https://enrichment.os.org")
        yield result

    def expand(self, entity: CE, match: CE) -> Generator[CE, None, None]:
        yield match


def test_enrich_process(vdataset, enricher):
    run_dataset(vdataset)

    stats = run_dataset(enricher)
    assert stats.entities > 0, stats.entities
