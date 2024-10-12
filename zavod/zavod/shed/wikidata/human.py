from typing import Optional
from datetime import timedelta
# from fingerprints import clean_brackets

from nomenklatura.enrich.wikidata import WikidataEnricher
from nomenklatura.enrich.wikidata.model import Item

from zavod import Context, Entity
from zavod.meta import Dataset
from zavod.shed.wikidata.country import is_historical_country, item_countries
from zavod.shed.wikidata.util import item_labels, item_types

Wikidata = WikidataEnricher[Dataset]
BLOCKED_PERSONS = {"Q1045488"}


def wikidata_basic_human(
    context: Context, enricher: Wikidata, item: Item, strict: bool = False
) -> Optional[Entity]:
    if item.id in BLOCKED_PERSONS:
        return None
    types = item_types(enricher, item.id)
    if "Q5" not in types:
        return None
    if "Q4164871" in types:  # human is also a position
        return None
    if "Q95074" in types:  # fictional character
        return None
    entity = context.make("Person")
    entity.id = item.id
    entity.add("wikidataId", item.id)

    is_dated = False
    is_historical = False
    for claim in item.claims:
        # P569 - birth date
        if claim.property == "P569":
            too_young = context.data_time - timedelta(days=365 * 18)
            too_old = context.data_time - timedelta(days=365 * 110)
            date = claim.text(enricher)
            if date.text is None:
                continue
            # Skip people from too far ago
            if date.text < "1900-01-01":
                return None
            if strict and date.text > too_young.isoformat():
                # context.log.warning("Person is too young", qid=item.id, date=date.text)
                return None
            if date.text < too_old.isoformat():
                # context.log.warning("Person is too old", qid=item.id, date=date.text)
                return None
            is_dated = True
            entity.add("birthDate", date.text)

        # P570 - death date
        if claim.property == "P570":
            date = claim.text(enricher)
            # context.log.warning("Person is dead", qid=item.id, date=date)
            if strict and date.text is not None:
                return None
            entity.add("deathDate", date.text)
            is_dated = True

        # P27 - citizenship
        if claim.property == "P27":
            if is_historical_country(enricher, claim.qid):
                is_historical = True
            elif claim.qid is not None:
                citizenship = enricher.fetch_item(claim.qid)
                if citizenship is not None:
                    for text in item_countries(enricher, citizenship):
                        text.apply(entity, "citizenship")

    # No DoB/DoD, but linked to a historical country - skip:
    if strict and (not is_dated and is_historical):
        return None

    for label in item_labels(item):
        label.apply(entity, "name")
        if label.lang == "eng":
            break

    return entity
