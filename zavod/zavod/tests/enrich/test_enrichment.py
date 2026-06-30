from typing import Generator

from followthemoney import SE
from nomenklatura.enrich import Enricher
from nomenklatura.judgement import Judgement
from normality import slugify

from zavod.archive import iter_dataset_statements
from zavod.crawl import crawl_dataset
from nomenklatura.db import make_session
from zavod.integration.dedupe import get_resolver
from zavod.meta import Dataset


class StubEnricher(Enricher):
    __test__ = False

    def match(self, entity: SE) -> Generator[SE, None, None]:
        if entity.schema.name != "Person":
            return
        result = self.make_entity(entity, "Person")
        derived_name = slugify(entity.first("name"))
        result.id = f"enrich-{derived_name}"
        result.add("name", entity.get("name"))
        result.add("birthDate", entity.get("birthDate"))
        result.add("sourceUrl", "https://enrichment.os.org")
        yield result

    def expand(self, entity: SE, match: SE) -> Generator[SE, None, None]:
        yield match
        relative = self.make_entity(match, "Person")
        relative.id = f"{match.id}-relative"
        relative.add("name", "Enriched Relative")
        yield relative

        family = self.make_entity(match, "Family")
        family.id = f"{match.id}-family"
        family.add("person", match.id)
        family.add("relative", relative.id)
        yield family


def test_enrich_process(testdataset1: Dataset, enricher: Dataset):
    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        assert list(resolver.get_candidates()) == []
        assert list(resolver.get_judgements()) == []
    crawl_dataset(testdataset1)

    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        assert list(resolver.get_candidates()) == []
        assert list(resolver.get_judgements()) == []
    stats = crawl_dataset(enricher)
    assert stats.entities > 0, stats.entities
    internals = list(iter_dataset_statements(enricher, external=False))
    assert len(internals) == 0, internals
    externals = list(iter_dataset_statements(enricher, external=True))
    assert len(externals) > 5, externals

    # Now merge one of the enriched entities with an internal one. The `with`
    # commits the judgement on exit, releasing the connection before the enrich
    # crawl below opens its own session.
    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        canon_id = resolver.decide(
            "osv-john-doe",
            "enrich-john-doe",
            Judgement.POSITIVE,
        )
        assert canon_id.id.startswith("NK-")
        assert len(resolver.connected(canon_id)) == 3
    stats = crawl_dataset(enricher)
    internals = list(iter_dataset_statements(enricher, external=False))
    assert len(internals) > 2, internals
    internal_ids = {statement.entity_id for statement in internals}
    assert "enrich-john-doe" in internal_ids
    assert "enrich-john-doe-relative" not in internal_ids
    assert "enrich-john-doe-family" not in internal_ids

    all_statements = list(iter_dataset_statements(enricher, external=True))
    external_ids = {
        statement.entity_id for statement in all_statements if statement.external
    }
    assert "enrich-john-doe-relative" in external_ids
    assert "enrich-john-doe-family" in external_ids
