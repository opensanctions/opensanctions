from functools import lru_cache
from nomenklatura.wikidata import WikidataClient, LangText
from rigour.territories import get_territory_by_qid

# Places we refuse to derive a country from, because their P17 claims name every
# state they span. Cultural and supranational regions only, plus historical
# territories `is_historical_country` misses - contested subdivisions keep all of
# their claimants deliberately.
SKIP_PLACES: set[str] = {
    "Q234",  # Flanders (cultural region: BE, FR, NL)
    "Q210718",  # Asia
    "Q4412",  # West Africa
    "Q52062",  # Nordic countries
    "Q7785",  # Commonwealth of Nations
    "Q4264",  # Mercosur
    "Q18348382",  # Colony of New South Wales
    "Q2334526",  # Province of North Carolina
    "Q1070529",  # Colony of Virginia
}


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
def item_countries(client: WikidataClient, qid: str) -> set[LangText]:
    """Extract the countries linked to an item, traversing up an administrative hierarchy
    via jurisdiction/part of properties."""
    return _crawl_item_countries(client, qid, (qid,))


def _crawl_item_countries(
    client: WikidataClient, qid: str, seen: tuple[str, ...]
) -> set[LangText]:
    if qid in SKIP_PLACES:
        return set()
    item = client.fetch_item(qid)
    if item is None:
        return set()
    countries: set[LangText] = set()
    territory = get_territory_by_qid(item.id)
    if territory is not None and territory.ftm_country is not None:
        text = LangText(territory.ftm_country, original=item.id)
        return set([text])

    next_seen = seen + (qid,)
    for claim in item.claims:
        # country:
        if claim.property in ("P17", "P27"):
            if claim.is_ended():
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
            if claim.is_ended() or claim.qid is None:
                continue
            if claim.qid in next_seen:
                continue
            # waaa_seen = next_seen + (claim.property,)
            countries.update(_crawl_item_countries(client, claim.qid, next_seen))
            if len(countries) > 0:
                return countries
    return countries
