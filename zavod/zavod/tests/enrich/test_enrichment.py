from typing import Generator

from nomenklatura import Resolver
from nomenklatura.enrich import Enricher
from nomenklatura.entity import CE
from nomenklatura.judgement import Judgement
from normality import slugify
from sqlalchemy import MetaData, create_engine

from zavod.archive import iter_dataset_statements
from zavod.crawl import crawl_dataset
from zavod.integration.dedupe import get_resolver
from zavod.meta import Dataset


class StubEnricher(Enricher):
    __test__ = False

    def match(self, entity: CE) -> Generator[CE, None, None]:
        if entity.schema.name != "Person":
            return
        result = self.make_entity(entity, "Person")
        derived_name = slugify(entity.first("name"))
        result.id = f"enrich-{derived_name}"
        result.add("name", entity.get("name"))
        result.add("birthDate", entity.get("birthDate"))
        result.add("sourceUrl", "https://enrichment.os.org")
        yield result

    def expand(self, entity: CE, match: CE) -> Generator[CE, None, None]:
        yield match


def test_enrich_process(testdataset1: Dataset, enricher: Dataset, disk_db_uri: str):
    resolver = get_resolver()

    resolver.begin()
    assert len(resolver.get_edges()) == 0, resolver.get_edges()
    resolver.rollback()
    crawl_dataset(testdataset1)

    resolver.begin()
    assert len(resolver.get_edges()) == 0, resolver.get_edges()
    resolver.rollback()
    stats = crawl_dataset(enricher)
    assert stats.entities > 0, stats.entities
    internals = list(iter_dataset_statements(enricher, external=False))
    assert len(internals) == 0, internals
    externals = list(iter_dataset_statements(enricher, external=True))
    assert len(externals) > 5, externals

    # Now merge one of the enriched entities with an interal one:
    resolver.begin()
    canon_id = resolver.decide(
        "osv-john-doe",
        "enrich-john-doe",
        Judgement.POSITIVE,
    )
    assert canon_id.id.startswith("NK-")
    assert len(resolver.connected(canon_id)) == 3
    resolver.commit()
    stats = crawl_dataset(enricher)
    internals = list(iter_dataset_statements(enricher, external=False))
    assert len(internals) > 2, internals
