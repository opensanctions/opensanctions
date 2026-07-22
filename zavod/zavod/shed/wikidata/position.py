from datetime import datetime
from typing import Dict, Optional, Set

from followthemoney import registry
from nomenklatura.wikidata import Item, WikidataClient, Claim
from nomenklatura.wikidata.lang import MULTI_LANG
from nomenklatura.wikidata.value import clean_wikidata_name
from rigour.territories import get_territory_by_qid
from rigour.time import iso_datetime

from zavod import Context, Entity
from zavod import helpers as h
from zavod.constants import ORIGIN_INFERRED
from zavod.shed.trans import translate_position_name
from zavod.shed.wikidata.client import WIKIDATA_QUERY_CACHE
from zavod.util import LangText
from zavod.stateful.positions import categorise, categorise_many
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

# Positions dissolved before this date never confer PEP status; the cutoff
# marks the internationally recognized end of history. Positions abolished
# *after* it still matter — living former holders remain PEPs.
POSITION_ABOLISHED_CUTOFF = "1990-12-26"

# Ancestor classes (matched against the item's full P31/P279 closure) whose
# descendants are categorically never PEP positions. Exclusion is silent —
# candidates hit by it never reach holder collection or the review UI — so
# the bar for adding entries is high: only classes that are unambiguously
# non-PEP-conferring belong here. When in doubt, leave the candidate to the
# review workflow, where the decision is recorded and reversible.
EXCLUDE_TYPES: Set[str] = {
    "Q114962596",  # historical position
    "Q193622",  # order
    "Q60754876",  # grade of an order
    "Q618779",  # award
    "Q13424289",  # honorary title (e.g. "national hero" designations)
    "Q4240305",  # cross
    "Q120560",  # minor basilica?
    "Q2977",  # cathedral
    "Q3320743",  # title of honor
    "Q42603",  # priest
    "Q11773926",  # ecclesiastical occupation
    "Q63187345",  # religious occupation
}

# Types that keep an item in the candidate set even when its ancestry hits
# EXCLUDE_TYPES or misses POSITION_BASICS (allow beats exclude; a reviewed
# position DB row beats both — see the `vetted` flag on wikidata_position).
ALLOW_TYPES: Set[str] = {
    # Members of the College of Cardinals are recognised by the Holy See as
    # PEPs, despite their religious-occupation ancestry:
    "Q45722",  # cardinal
    "Q1729113",  # cardinal-bishop
    "Q2033341",  # cardinal priest
    "Q2361374",  # cardinal-deacon
    "Q19808790",  # Episcopal Co-Prince (joint head of state of Andorra)
}
ALLOW_TYPES.update(SUB_TYPES.keys())

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
    # Precedence: a position DB verdict beats the type-based heuristics below,
    # and ALLOW_TYPES beats EXCLUDE_TYPES. The DB check also runs first so a
    # reviewed-rejected position skips the more expensive work (country
    # lookups, translation).
    existing = categorise_many(context, [item.id])
    if len(existing) > 0 and existing[0].is_pep is False:
        return None
    db_is_pep = len(existing) > 0 and existing[0].is_pep is True

    types = item.types
    if not db_is_pep and types.isdisjoint(ALLOW_TYPES):
        if types.isdisjoint(POSITION_BASICS):
            return None
        if not types.isdisjoint(EXCLUDE_TYPES):
            return None

    position = context.make("Position")
    position.id = item.id
    position.add("wikidataId", item.id)

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

    # Positions dissolved before the cutoff are dropped — unless the position
    # DB explicitly marks them as PEP-conferring (living former holders can
    # remain PEPs regardless of when their position was abolished):
    end_date = max(position.get("dissolutionDate"), default=None)
    if end_date is not None and end_date < POSITION_ABOLISHED_CUTOFF and not db_is_pep:
        return None

    if item.label is not None and item.label.text is not None:
        # item.label is picked from the available labels in PREFERRED_WD_LANGS order
        # (English first, then "mul"/multilingual, then the next-best preferred
        # language). English and multilingual labels can be used as-is; anything
        # else is the next-best language Wikidata gave us, and we translate it to
        # English. Picking what to translate may get more complex in the future,
        # but for now translating the next-best pick after English works for us.
        if item.label.lang in ("eng", MULTI_LANG, None):
            item.label.apply(position, "name", clean=clean_wikidata_name)
        else:
            clean_label_text = clean_wikidata_name(item.label.text)
            if clean_label_text is not None and clean_label_text.strip() != "":
                assert item.label.lang is not None
                result = translate_position_name(
                    context,
                    LangText(text=item.label.text, lang=item.label.lang),
                )
                translated = result.get_preferred_language()
                # if for some reason the translation fails, fall back to the original
                if translated is None:
                    item.label.apply(position, "name", clean=clean_wikidata_name)
                else:
                    position.add(
                        "name",
                        translated.text,
                        lang=translated.lang,
                        original_value=item.label.text,
                        origin=result.origin,
                    )

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
    categorisation = categorise(context, position, default_is_pep=is_pep)
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


def position_holders(
    client: WikidataClient, item: Item
) -> Dict[str, Optional[datetime]]:
    """Find persons who have held the position defined by `item`, combining the
    inverted lookup on property P39 (position held) with the position item's own
    P1308 (officeholder) claims.

    Returns a dict mapping person QID → schema:dateModified timestamp (ISO 8601);
    the timestamp is None for holders known only via P1308, so their cached item
    expires on the regular schedule instead of being refreshed on change.
    """
    query = f"""
    SELECT ?person ?modifiedAt WHERE {{
        ?person wdt:P39 wd:{item.id} .
        ?person wdt:P31 wd:Q5 .
        ?person schema:dateModified ?modifiedAt .
    }}
    """
    holders: Dict[str, Optional[datetime]] = {}
    # Holder lists (and the dateModified values that drive person cache
    # invalidation) change slowly; at 1 day, every crawl re-runs tens of
    # thousands of WDQS queries.
    response = client.query(query, cache_days=WIKIDATA_QUERY_CACHE)
    for result in response.results:
        person_qid = result.plain("person")
        modified_at = result.plain("modifiedAt")
        if person_qid is not None:
            holders[person_qid] = iso_datetime(modified_at)

    for claim in item.claims:
        if claim.property == "P1308" and claim.qid is not None:
            holders.setdefault(claim.qid, None)

    return holders


def wikidata_occupancy(
    context: Context, person: Entity, position: Entity, claim: Claim
) -> Optional[Entity]:
    """Create an Occupancy entity for the given person and position based on the claim,
    which identifies relevant qualifiers."""
    start_date: Optional[str] = None
    for qual in claim.get_qualifier("P580"):
        qual_date = qual.text.text
        if qual_date is not None:
            if start_date is None:
                start_date = qual_date
            else:
                start_date = min(start_date, qual_date)

    end_date: Optional[str] = None
    for qual in claim.get_qualifier("P582"):
        qual_date = qual.text.text
        if qual_date is not None:
            if end_date is None:
                end_date = qual_date
            else:
                end_date = max(end_date, qual_date)

    # Set the key prefix in order to avoid duplicating occupancies for the same
    # position held by the same person across multiple datasets. The choice is
    # somewhat arbitrary, but it avoids a larger delta if we chose "wikidata".
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        start_date=start_date,
        end_date=end_date,
        key_prefix="wd_peps",
    )

    if occupancy is None:
        return None

    # Wikidata persons frequently lack their own citizenship statement, so we
    # associate confirmed PEPs with the position's country. Diplomatic posts
    # (role.diplo) name the receiving country rather than the person's, so those
    # are left out.
    if "role.diplo" not in position.get("topics"):
        for country in position.get("country"):
            if country not in person.get_type_values(registry.country, matchable=True):
                person.add("country", country, origin=ORIGIN_INFERRED)

    # reference URL:
    for ref in claim.references:
        for snak in ref.get("P854"):
            if snak.text is not None:
                snak.text.apply(occupancy, "sourceUrl")

    # electoral district:
    for qual in claim.get_qualifier("P768"):
        if qual.text is not None:
            qual.text.apply(occupancy, "constituency")

    return occupancy
