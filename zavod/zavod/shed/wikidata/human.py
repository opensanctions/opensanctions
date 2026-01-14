from typing import Optional
from datetime import timedelta
from followthemoney import registry
from nomenklatura.wikidata import Item, WikidataClient
from nomenklatura.wikidata.value import clean_wikidata_name

from zavod import settings
from zavod import Context, Entity
from zavod.shed.wikidata.country import is_historical_country, item_countries

BLOCKED_PERSONS = {"Q1045488"}


def wikidata_basic_human(
    context: Context, client: WikidataClient, item: Item, strict: bool = False
) -> Optional[Entity]:
    if item.id in BLOCKED_PERSONS:
        return None
    types = item.types
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
            too_young = settings.RUN_TIME - timedelta(days=365 * 18)
            too_old = settings.RUN_TIME - timedelta(days=365 * 110)
            date = claim.text
            if date.text is None:
                continue
            # Skip people from too far ago
            if date.text < registry.date.RELEVANCE_MIN:
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
            date = claim.text
            # context.log.warning("Person is dead", qid=item.id, date=date)
            if strict and date.text is not None:
                return None
            entity.add("deathDate", date.text)
            is_dated = True

        # P27 - citizenship
        if claim.property == "P27" and claim.qid is not None:
            if is_historical_country(client, claim.qid):
                is_historical = True

            for text in item_countries(client, claim.qid):
                text.apply(entity, "citizenship")

    # Try increasingly more desperate ways to find a country linked to the person:
    if not entity.has("citizenship"):
        # other country properties
        for prop in ("P1001", "P17", "P19", "P945", "P495", "P937"):
            for claim in item.claims:
                if claim.qid is None:
                    continue
                if claim.property == prop:
                    for text in item_countries(client, claim.qid):
                        text.apply(entity, "country")
            if entity.has("country"):
                break

    # No DoB/DoD, but linked to a historical country - skip:
    if strict and (not is_dated and is_historical):
        return None

    if item.label is not None:
        item.label.apply(entity, "name", clean=clean_wikidata_name)

    return entity
