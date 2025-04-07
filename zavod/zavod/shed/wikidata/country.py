from typing import Set, Tuple
from functools import lru_cache
from nomenklatura.wikidata import WikidataClient, LangText
from rigour.territories import get_territory_by_qid


@lru_cache(maxsize=5000)
def is_historical_country(client: WikidataClient, qid: str) -> bool:
    territory = get_territory_by_qid(qid)
    if territory is not None:
        return territory.is_historical
    item = client.fetch_item(qid)
    if item is None:
        return False
    types = item.types
    if "Q3024240" in types:  # historical country
        return True
    if "Q19953632" in types:  #  former administrative territorial entity
        return True
    if "Q839954" in types:  # archeological site
        return True
    return False


@lru_cache(maxsize=5000)
def item_countries(client: WikidataClient, qid: str) -> Set[LangText]:
    """Extract the countries linked to an item, traversing up an administrative hierarchy
    via jurisdiction/part of properties."""
    return _crawl_item_countries(client, qid, (qid,))


def _crawl_item_countries(
    client: WikidataClient, qid: str, seen: Tuple[str, ...]
) -> Set[LangText]:
    item = client.fetch_item(qid)
    if item is None:
        return set()
    countries: Set[LangText] = set()
    territory = get_territory_by_qid(item.id)
    if territory is not None and territory.ftm_country is not None:
        text = LangText(territory.ftm_country, original=item.id)
        return set([text])

    next_seen = seen + (qid,)
    for claim in item.claims:
        # country:
        if claim.property in ("P17", "P27"):
            if claim.qualifiers.get("P582"):
                continue
            if claim.qid is None or claim.qid in next_seen:
                continue
            countries.update(_crawl_item_countries(client, claim.qid, next_seen))
    if len(countries) > 0:
        return countries

    # jurisdiction, capital of, part of:
    for prop in ("P1001", "P1376", "P361", "P749", "P159", "P2389"):
        for claim in item.claims:
            if claim.property != prop:
                continue
            if claim.qualifiers.get("P582") or claim.qid is None:
                continue
            if claim.qid in next_seen:
                continue
            # waaa_seen = next_seen + (claim.property,)
            countries.update(_crawl_item_countries(client, claim.qid, next_seen))
            if len(countries) > 0:
                return countries
    return countries
