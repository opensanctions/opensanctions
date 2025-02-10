from typing import NamedTuple, Optional, Set
from functools import lru_cache
from followthemoney.types.country import CountryType
from nomenklatura.enrich.wikidata import WikidataEnricher
from nomenklatura.enrich.wikidata.lang import LangText
from nomenklatura.enrich.wikidata.model import Item

from zavod import Context
from zavod.meta import Dataset
from zavod.shed.wikidata.query import run_raw_query
from zavod.shed.wikidata.util import item_types, item_label


Wikidata = WikidataEnricher[Dataset]
countries_type = CountryType()

SPECIAL_COUNTRIES = {
    "Q15180",  # Soviet Union
    "Q237",  # Vatican city
    "Q36704",  # Yugoslavia
    "Q458",  # European Union
    "Q23681",  # Northern Cyprus
    "Q219",  # Bulgaria
    "Q8646",  # Hong Kong
    "Q14773",  # Macao
    "Q865",  # Taiwan
    "Q33946",  # Czechoslovakia
    "Q1246",  # Kosovo
    "Q17269",  # Tibet autonomous region
    "Q2444884",  # Tibet
    "Q907112",  # Transnistria
    "Q31354462",  # Abkhazia
    "Q2914461",  # Autonomous Republic of Abkhazia
    "Q244165",  # Republic of Artsakh
    "Q23427",  # South Ossetia
    "Q16746854",  # Luhansk PR
    "Q16150196",  # Donetsk PR
    "Q15966495",  # Crimea Rep
    "Q174193",  # United Kingdom of Great Britain and Ireland
    "Q174193",  # United Kingdom of Great Britain and Ireland
    "Q25",  # Wales
    "Q22",  # Scotland
    "Q26",  # Northern Ireland
    "Q3405693",  # Sark
    "Q25230",  # Bailiwick of Guernsey
    "Q9676",  # Isle of Man
    "Q785",  # Jersey
    "Q347",  # Liechtenstein
    "Q46197",  # Ascension Island
    "Q223",  # Greenland
    "Q25231",  # Svalbard
    "Q23408",  # Bouvet Island
    "Q4628",  # Faroe Islands
    "Q5689",  # Aland Islands
    "Q5813",  # Canary Islands
    "Q25279",  # Curacao
    "Q160016",  # member states of the United Nations
    "Q61964031",  # member of the UN SC
    "Q34754",  # Somaliland
    "Q11703",  # US Virgin Islands
    "Q184851",  # Diego Garcia
    "Q1183",  # Puerto Rico
    "Q16645",  # United States Minor Outlying Islands
    "Q6250",  # Western sahara
    "Q40362",  # Sahrawi Arab Democratic Republic
    "Q131198",  # Heard Island and McDonald Islands
    "Q36004",  # Cocos (Keeling) Islands
    "Q31057",  # Norfolk Island
    "Q31063",  # Christmas Island
    "Q220982",  # Tristan da Cunha
    "Q36823",  # Tokelau
    "Q17012",  # Guadeloupe
    "Q35555",  # Wallis and Futuna
    "Q33788",  # New Caledonia
    "Q17054",  # Martinique
    "Q3769",  # French Guiana
    "Q126125",  # Saint Martin
    "Q17070",  # Reunion
    "Q17063",  # Mayotte
    "Q34617",  # Saint Pierre and Miquelon
    "Q25362",  # Saint Barthelemy
    "Q30971",  # French Polynesia
    "Q129003",  # French Southern and Antarctic Lands
    "Q27561",  # Caribbean Netherlands
    "Q161258",  # Clipperton Island
    "Q43100",  # Kashmir
}
BLOCK_ITEMS = {
    "Q2961631",  # Chaumontois
    "Q124153644",  # Chinland
    "Q125422413",  # Persia
    "Q126362486",  # Kerajaan Patipi
    "Q37362",  # Akrotiri and Dhekelia
    "Q131008",  # Johnston Atoll
    "Q498979",  # Panama Canal Zone
    "Q107357273",  # United States Pacific Island Wildlife Refuges
}
SKIP_CODES = {"zz", "dd", "csxx", "zr"}


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
    return False


@lru_cache(maxsize=2000)
def is_country(enricher: Wikidata, qid: str) -> bool:
    if qid in SPECIAL_COUNTRIES:
        return True
    if qid in BLOCK_ITEMS:
        return False
    if is_historical_country(enricher, qid):
        return False

    item = enricher.fetch_item(qid)
    if item is None:
        return False
    # We only want countries, not concepts (which would carry a P279 "subclass of" )
    instance_of_types = {claim.qid for claim in item.claims if claim.property == "P31"}

    deny = {
        "Q12885585",  # Native American tribe
        "Q1145276",  # fictional country
    }
    if len(instance_of_types.intersection(deny)) > 0:
        return False

    allow = (
        "Q6256",  # country
        "Q3624078",  # sovereign state
        # "Q1048835",  # political territorial entity
        # "Q56061",  # administrative territorial entity
        "Q10711424",  # state with limited recognition
        "Q15239622",  # disputed territory
        # "Q1335818",  # supranational union
        "Q46395",  # British Overseas Territory
        "Q783733",  # Unincorporated US territory
    )
    if len(instance_of_types.intersection(allow)) > 0:
        return True
    return False


def item_countries(enricher: Wikidata, item: Item) -> Set[LangText]:
    """Extract the countries linked to an item, traversing up an administrative hierarchy
    via jurisdiction/part of properties."""
    countries: Set[LangText] = set()
    if is_country(enricher, item.id):
        text = item_label(item)
        if text is not None:
            return set([text])

    for claim in item.claims:
        # country:
        if claim.property in ("P17", "P27"):
            if claim.qualifiers.get("P582"):
                continue
            if not is_country(enricher, claim.qid):
                continue
            text = claim.text(enricher)
            countries.add(text)
    if len(countries) > 0:
        return countries
    for claim in item.claims:
        # jurisdiction, capital of, part of:
        if claim.property in ("P1001", "P1376", "P361"):
            if claim.qualifiers.get("P582") or claim.qid is None:
                continue
            # if claim.qid in seen:
            #     continue
            subitem = enricher.fetch_item(claim.qid)
            if subitem is None:
                continue
            # print("SUBITEM", repr(subitem))
            # subseen = seen + [claim.qid]
            countries.update(item_countries(enricher, subitem))
    return countries


def all_countries(context: Context, enricher: Wikidata) -> Set[Country]:
    countries: Set[Country] = set()
    for qid in SPECIAL_COUNTRIES:
        item = enricher.fetch_item(qid)
        if item is not None:
            text = item_label(item)
            if text is not None:
                code = countries_type.clean(text.text)
                countries.add(Country(qid, code, text.text or qid))

    query = """
    SELECT ?country WHERE {
        VALUES ?type { wd:Q15634554 wd:Q3624078 wd:Q6256 wd:Q46395 wd:Q783733 }
        ?country wdt:P31 ?type .
    }
    """
    response = run_raw_query(context, query)
    for result in response.results:
        rqid = result.plain("country")
        if rqid is None or not is_country(enricher, rqid):
            continue
        item = enricher.fetch_item(rqid)
        if item is None:
            continue
        text = item_label(item)
        if text is not None:
            code = countries_type.clean(text.text)
            if code is None:
                context.log.warn(
                    "Country name does not map to code sheet",
                    name=text.text,
                    qid=item.id,
                )
                continue
            countries.add(Country(qid, code, text.text or qid))

    reference = dict(countries_type.names.items())
    for country in countries:
        cc = countries_type.clean(country.label)
        if cc in reference:
            reference.pop(cc)

    for code, name in reference.items():
        if code in SKIP_CODES:
            continue
        context.log.warning(
            "Country/territory does not have a QID",
            name=name,
            code=code,
        )

    return countries
