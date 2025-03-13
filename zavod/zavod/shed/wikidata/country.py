from typing import NamedTuple, Optional, Set
from functools import lru_cache
from followthemoney.types.country import CountryType
from nomenklatura.enrich.wikidata import WikidataEnricher
from nomenklatura.enrich.wikidata.lang import LangText
from nomenklatura.enrich.wikidata.model import Item
from rigour.territories import get_territory_by_qid

from zavod.meta import Dataset
from zavod.shed.wikidata.util import item_types


Wikidata = WikidataEnricher[Dataset]
countries_type = CountryType()


class Country(NamedTuple):
    qid: str
    code: Optional[str]
    label: str


@lru_cache(maxsize=2000)
def is_historical_country(enricher: Wikidata, qid: str) -> bool:
    types = item_types(enricher, qid)
    if "Q3024240" in types:  # historical country
        return True
    if "Q19953632" in types:  #  former administrative territorial entity
        return True
    if "Q839954" in types:  # archeological site
        return True
    territory = get_territory_by_qid(qid)
    if territory is not None and territory.is_historical:
        return True
    return False


def item_countries(
    enricher: Wikidata, item: Item, seen: Optional[Set[str]] = None
) -> Set[LangText]:
    """Extract the countries linked to an item, traversing up an administrative hierarchy
    via jurisdiction/part of properties."""
    countries: Set[LangText] = set()
    territory = get_territory_by_qid(item.id)
    if territory is not None and territory.ftm_country is not None:
        text = LangText(territory.ftm_country, "en", original=item.id)
        return set([text])

    for claim in item.claims:
        # country:
        if claim.property in ("P17", "P27"):
            if claim.qualifiers.get("P582"):
                continue
            territory = get_territory_by_qid(item.id)
            if territory is not None and territory.ftm_country is not None:
                text = LangText(territory.ftm_country, "en", original=item.id)
                countries.add(text)
    if len(countries) > 0:
        return countries
    seen = set() if seen is None else set(seen)
    seen.add(item.id)
    for claim in item.claims:
        # jurisdiction, capital of, part of:
        if claim.property in ("P1001", "P1376", "P361"):
            if claim.qualifiers.get("P582") or claim.qid is None:
                continue
            if claim.qid in seen:
                continue
            subitem = enricher.fetch_item(claim.qid)
            if subitem is None:
                continue
            countries.update(item_countries(enricher, subitem, seen=seen))
    return countries
