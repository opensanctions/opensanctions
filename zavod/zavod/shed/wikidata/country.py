from functools import lru_cache
from nomenklatura.wikidata import WikidataClient, LangText
from rigour.territories import get_territory_by_qid
from rigour.territories.territory import Territory

# Positions dissolved before this date never confer PEP status; the cutoff
# marks the internationally recognized end of history. Positions abolished
# *after* it still matter — living former holders remain PEPs.
POSITION_ABOLISHED_CUTOFF = "1990-12-26"

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
    """Check whether a QID names a polity that ended before the PEP cutoff, so that
    claims pointing at it can be ignored when deriving countries.

    A wrong "historical" verdict drops countries, positions and living PEPs, while
    a wrong "current" one only lets extra candidates through to later checks. So
    beyond rigour's own verdict, a polity only counts as historical if Wikidata
    types it as such *and* dates its end before `POSITION_ABOLISHED_CUTOFF`.
    This also covers rigour's alternate QIDs, which include predecessor states
    (the German Reich under `de`) mapped to a current territory."""
    territory = get_territory_by_qid(qid)
    if territory is not None and territory.is_historical:
        return True
    item = client.fetch_item(qid)
    if item is None:
        return False
    types = item.types
    if "Q839954" in types:  # archeological site
        return True
    if "Q3024240" not in types and "Q19953632" not in types:
        # neither historical country nor former administrative territorial entity
        return False
    end_dates: list[str] = []
    for claim in item.claims:
        if claim.property in ("P576", "P582") and claim.text.text is not None:
            end_dates.append(claim.text.text)
    end_date = max(end_dates, default=None)
    return end_date is not None and end_date < POSITION_ABOLISHED_CUTOFF


def current_territory_qids(client: WikidataClient, territory: Territory) -> list[str]:
    """List the QIDs to sweep when querying Wikidata for a territory: its primary
    QID and those alternates that don't name a historical polity."""
    return [q for q in sorted(territory.qids) if not is_historical_country(client, q)]


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
            if is_historical_country(client, claim.qid):
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
