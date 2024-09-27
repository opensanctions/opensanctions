from typing import Optional

from nomenklatura.enrich.wikidata import WikidataEnricher
from nomenklatura.enrich.wikidata.model import Item

from zavod import Context, Entity
from zavod.logic.pep import categorise
from zavod.shed.wikidata.util import item_types, item_countries, item_label

Wikidata = WikidataEnricher[Entity]

POSITION_BASICS = {
    "Q4164871",  # position
    "Q29645880",  # ambassador of a country
    "Q29645886",  # ambassador to a country
    "Q707492",  # military chief of staff
}
SUB_TYPES = {
    "Q30185": "gov.muni",  # mayor
    "Q2285706": ("role.pep", "gov.head"),  # head of government
    "Q48352": ("role.pep", "gov.head"),  # head of state
    "Q4175034": "gov.legislative",  # legislator
    "Q486839": ("role.pep", "gov.legislative"),  # member of parliament
    "Q83307": ("role.pep", "gov.executive"),  # minister
    "Q7330070": ("role.pep", "gov.executive"),  # foreign minister
    "Q14212": ("gov.head", "gov.executive"),  # prime minister
    # "Q108290289": "role.pep",  # senior government officials
    "Q16533": "gov.judicial",  # judge
    "Q107363151": ("role.pep", "gov.financial"),  # central bank governor
    "Q1553195": "pol.party",  # party leader
    "Q116182667": "role.diplo",  # diplomat
    "Q29645880": ("role.pep", "role.diplo"),  # ambassador of a country
    "Q29645886": ("role.pep", "role.diplo"),  # ambassador to a country
    "Q303618": "role.diplo",  # diplomatic rank
    "Q707492": ("role.pep", "gov.security"),  # military chief of staff
}


def wikidata_position(
    context: Context, enricher: Wikidata, item: Item
) -> Optional[Entity]:
    types = item_types(enricher, item.id)
    if len(types.intersection(POSITION_BASICS)) == 0:
        return None
    if "Q114962596" in types:  # historical position
        return None

    position = context.make("Position")
    position.id = item.id
    position.add("wikidataId", item.id)
    label = item_label(item)
    if label is not None:
        label.apply(position, "name")
    else:
        for label in item.labels:
            label.apply(position, "name")

    countries = item_countries(enricher, item)
    for country in countries:
        country.apply(position, "country")

    # Skip all positions that cannot be linked to a country.
    if not position.has("country"):
        return None

    for sub_type, topics in SUB_TYPES.items():
        if sub_type in types:
            position.add("topics", topics)

    is_pep = True if "role.pep" in position.get("topics") else None
    categorisation = categorise(context, position, is_pep=is_pep)
    if not categorisation.is_pep:
        return None
    position.set("topics", categorisation.topics)
    return position
