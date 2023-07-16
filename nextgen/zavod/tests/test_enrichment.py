from typing import Generator
from nomenklatura.enrich import Enricher
from nomenklatura.judgement import Judgement
from nomenklatura.entity import CE

from zavod.meta import Dataset
from zavod.runner import run_dataset
from zavod.dedupe import get_resolver
from zavod.archive import iter_dataset_statements


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


def test_enrich_process(vdataset: Dataset, enricher: Dataset):
    resolver = get_resolver()
    run_dataset(vdataset)

    assert len(resolver.edges) == 0, resolver.edges
    stats = run_dataset(enricher)
    assert stats.entities > 0, stats.entities
    assert len(resolver.edges) > 0, resolver.edges
    internals = list(iter_dataset_statements(enricher, external=False))
    assert len(internals) == 0, internals
    externals = list(iter_dataset_statements(enricher, external=True))
    assert len(externals) > 5, externals

    # Now merge one of the enriched entities with an interal one:
    resolver.decide("osv-john-doe", "enrich-osv-john-doe", Judgement.POSITIVE)
    stats = run_dataset(enricher)
    internals = list(iter_dataset_statements(enricher, external=False))
    assert len(internals) > 2, internals
