from operator import is_
from nomenklatura.cache import Cache
from nomenklatura.util import is_qid
from nomenklatura.enrich.wikidata import WikidataEnricher
from nomenklatura.resolver import Identifier
from nomenklatura.judgement import Judgement

from opensanctions.core.logs import get_logger
from opensanctions.core.db import engine, metadata, engine_read
from opensanctions.core.statements import entities_datasets
from opensanctions.core.resolver import get_resolver
from opensanctions.core.dataset import Dataset

log = get_logger(__name__)


def get_wikidata_enricher() -> WikidataEnricher:
    wikidata = Dataset.require("wikidata")
    cache = Cache(engine, metadata, wikidata)
    wd: WikidataEnricher = wikidata.get_enricher(cache)
    return wd


def audit_resolver():
    wd = get_wikidata_enricher()
    resolver = get_resolver()

    log.info("Loading all entity IDs...")
    with engine_read() as conn:
        entities = list(entities_datasets(conn))

    entity_ids = set([e for e, _ in entities])
    log.info("Loaded %d entity IDs..." % len(entity_ids))

    canonicals = list(resolver.canonicals())
    for idx, canonical in enumerate(canonicals):
        if idx > 0 and idx % 10000 == 0:
            log.info("Processed: %d..." % idx)
        members = resolver.connected(canonical)
        qids = set()
        ofacs = 0
        for member in members:
            if is_qid(member.id):
                qid = member.id
                item = wd.fetch_item(qid)
                if item is None:
                    log.error("Missing WD item", qid=qid)
                if item.id != qid:
                    item = wd.fetch_item(qid, cache_days=0)
                    judgement = resolver.get_judgement(member, item.id)
                    if judgement != Judgement.POSITIVE:
                        resolver.decide(
                            member,
                            item.id,
                            judgement=Judgement.POSITIVE,
                            user="opensanctions-audit",
                        )
                    qid = item.id
                qids.add(qid)

            if member.id.startswith("ofac-"):
                ofacs += 1
            if member.id not in entity_ids:
                if member.canonical:
                    continue
                log.warn(
                    "Referenced entity does not exist",
                    canonical=canonical.id,
                    entity=member.id,
                )
                resolver.remove(member)

        if ofacs > 1:
            log.warning("More than one OFAC ID", id=canonical.id, size=len(members))

        if len(qids) > 1:
            log.error("Entity has more than one QID", qids=qids)

    resolver.save()
