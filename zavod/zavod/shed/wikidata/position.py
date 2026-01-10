from typing import Dict, Optional, Set

from nomenklatura.wikidata import Item, WikidataClient, Claim
from nomenklatura.wikidata.value import clean_wikidata_name
from rigour.territories import get_territory_by_qid

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.shed.wikidata.country import is_historical_country, item_countries

POSITION_BASICS: Set[str] = {
    "Q4164871",  # position
    "Q29645880",  # ambassador of a country
    "Q29645886",  # ambassador to a country
    "Q707492",  # military chief of staff
}
SUB_TYPES: Dict[str, Set[str]] = {
    "Q30185": {"role.pep", "gov.executive", "gov.muni"},  # mayor
    "Q17279032": {"role.pep"},  # elective office
    "Q109862464": {"gov.executive", "gov.muni"},  # lord mayor
    "Q2285706": {"role.pep", "gov.head"},  # head of government
    "Q48352": {"role.pep", "gov.head"},  # head of state
    "Q3099723": {"role.pep", "gov.head"},  # minister-president
    "Q4175034": {"gov.legislative"},  # legislator
    "Q486839": {"role.pep", "gov.legislative"},  # member of parliament
    "Q83307": {"role.pep", "gov.executive"},  # minister
    "Q7330070": {"role.pep", "gov.executive"},  # foreign minister
    "Q14212": {"gov.head", "gov.executive"},  # prime minister
    "Q15966511": {"role.pep", "gov.executive", "gov.state"},  # deputy minister
    "Q132050": {"role.pep", "gov.executive"},  # governor
    "Q26204040": {"role.pep", "gov.executive"},  # deputy minister
    "Q46403368": {"role.pep", "gov.national"},  # deputy at the national level
    "Q20086425": {"role.pep", "gov.legislative"},  # shadow minister
    "Q303329": {"role.pep", "gov.legislative"},  # shadow cabinet
    "Q108290289": {"role.pep"},  # senior government officials
    "Q16533": {"gov.judicial"},  # judge
    "Q6635529": {
        "role.pep",
        "gov.executive",
    },  # provincial leader of the People's Republic of China
    "Q3526627": {
        "role.pep",
        "gov.legislative",
        "gov.state",
    },  # member of a Legislative Assembly of India
    "Q117826617": {"role.pep", "gov.judicial"},  # supreme court judge
    "Q55736868": {
        "role.pep",
        "gov.judicial",
        "gov.national",
    },  # national sup court judge
    "Q1501926": {"role.pep", "gov.judicial"},  # attorney general
    "Q3368517": {"role.pep", "gov.judicial"},  # public prosecutor general
    "Q109607046": {"role.pep", "gov.judicial"},  # deputy public prosecutor general
    "Q107363151": {"role.pep", "gov.financial"},  # central bank governor
    "Q1553195": {"role.pep", "pol.party"},  # party leader
    "Q836971": {"pol.party"},  # party secretary
    "Q116182667": {"role.diplo"},  # diplomat
    "Q29645880": {"role.pep", "role.diplo"},  # ambassador of a country
    "Q29645886": {"role.pep", "role.diplo"},  # ambassador to a country
    "Q303618": {"role.diplo"},  # diplomatic rank
    "Q707492": {"role.pep", "gov.national", "gov.security"},  # military chief of staff
}

IGNORE_TYPES: Set[str] = {
    "Q114962596",  # historical position
    "Q193622",  # order
    "Q60754876",  # grade of an order
    "Q618779",  # award
    "Q4240305",  # cross
    "Q120560",  # minor basilica?
    "Q2977",  # cathedral
}

# TEMP: We're starting to include municipal PEPs for specific countries
MUNI_COUNTRIES = {
    "au",
    "be",
    "br",
    "by",
    "ca",
    "co",
    "cz",
    "es",
    "fr",
    "gb",
    "gt",
    "hu",
    "id",
    "is",
    "it",
    "ke",
    "kr",
    "mx",
    "ni",
    "nl",
    "pl",
    "ro",
    "ru",
    "sk",
    "ua",
    "us",
    "ve",
    "za",
}


def wikidata_position(
    context: Context, client: WikidataClient, item: Item
) -> Optional[Entity]:
    types = item.types
    if len(types.intersection(POSITION_BASICS)) == 0:
        return None
    if len(types.intersection(IGNORE_TYPES)) > 0:
        return None

    position = context.make("Position")
    position.id = item.id
    position.add("wikidataId", item.id)
    if item.label is not None:
        item.label.apply(position, "name", clean=clean_wikidata_name)

    for claim in item.claims:
        if claim.property in ("P1001", "P17", "P27") and claim.qid is not None:
            if is_historical_country(client, claim.qid):
                return None
            for country in item_countries(client, claim.qid):
                country.apply(position, "country")

        # jurisdiction:
        if claim.property == "P1001":
            territory = get_territory_by_qid(claim.qid)
            if territory is None or not territory.is_country:
                claim.text.apply(position, "subnationalArea")

        # inception:
        if claim.property == "P571":
            claim.text.apply(position, "inceptionDate")

        if claim.property == "P580":
            claim.text.apply(position, "inceptionDate")

        # abolished date:
        if claim.property == "P576":
            claim.text.apply(position, "dissolutionDate")

    # Second round:
    for claim in item.claims:
        # start date:
        if claim.property == "P580" and not position.has("inceptionDate"):
            claim.text.apply(position, "inceptionDate")

        # end date:
        if claim.property == "P582" and not position.has("dissolutionDate"):
            claim.text.apply(position, "dissolutionDate")

    # If no explicit country/jurisdiction is found, try to traverse more obscure
    # properties, like capital of, part of, jurisdiction, etc.
    if not position.has("country"):
        for country in item_countries(client, item.id):
            country.apply(position, "country")

    # Skip all positions that cannot be linked to a country.
    if not position.has("country"):
        return None

    # Check for the intl. recognized end of history:
    end_date = max(position.get("dissolutionDate"), default=None)
    if end_date is not None and end_date < "1990-12-26":
        return None

    topics: Set[str] = set()
    for sub_type, type_topics in SUB_TYPES.items():
        if sub_type in types:
            topics.update(type_topics)

    is_pep = "role.pep" in topics
    topics.discard("role.pep")

    if "gov.state" in topics:
        topics.discard("gov.muni")
    if "gov.national" in topics:
        topics.discard("gov.state")
    if "gov.igo" in topics:
        topics.discard("gov.national")
    # All mayors are also heads of local government, but that looks a bit silly:
    if "gov.muni" in topics:
        topics.discard("gov.head")

    position.add("topics", topics)
    categorisation = categorise(context, position, is_pep=is_pep)
    if not categorisation.is_pep:
        return None
    real_topics = set(categorisation.topics)
    real_topics.discard("role.pep")
    if "gov.muni" in real_topics:
        real_topics.discard("gov.head")
        if MUNI_COUNTRIES.isdisjoint(position.countries):
            return None

    position.set("topics", real_topics)
    return position


def position_holders(client: WikidataClient, item: Item) -> Set[str]:
    """Find persons who have held the position defined by `item`. This performs
    the inverted lookup on property P39 (position held). Independently, the crawler
    should check property P1308 (officeholder) on the position item itself.
    """
    query = f"""
    SELECT ?person WHERE {{
        ?person wdt:P39 wd:{item.id} .
        ?person wdt:P31 wd:Q5
    }}
    """
    persons: Set[str] = set()
    response = client.query(query)
    for result in response.results:
        person_qid = result.plain("person")
        if person_qid is not None:
            persons.add(person_qid)

    return persons


def wikidata_occupancy(
    context: Context, person: Entity, position: Entity, claim: Claim
) -> Optional[Entity]:
    """Create an Occupancy entity for the given person and position based on the claim,
    which identifies relevant qualifiers."""
    start_date: Optional[str] = None
    for qual in claim.qualifiers.get("P580", []):
        qual_date = qual.text.text
        if qual_date is not None:
            if start_date is None:
                start_date = qual_date
            else:
                start_date = min(start_date, qual_date)

    end_date: Optional[str] = None
    for qual in claim.qualifiers.get("P582", []):
        qual_date = qual.text.text
        if qual_date is not None:
            if end_date is None:
                end_date = qual_date
            else:
                end_date = max(end_date, qual_date)

    # Diplomatic positions tend to be associated with the receiving country,
    # so we don't want to propagate that to the person.
    position_topics = position.get("topics")
    is_diplomat = "role.diplo" in position_topics

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        start_date=start_date,
        end_date=end_date,
        propagate_country=not is_diplomat,
    )
    return occupancy
