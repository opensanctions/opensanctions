import json
import re
from rigour.mime.types import JSON

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


CURRENT_URL = "https://dati.senato.it/DatiSenato/browse/6?"
DATA = {
    "alias": "senatori-legislatura",
    "id": "2",
    "query_format": "json",
    "commit": "Download",
}
IGNORE = [
    "tipoMandato",  # mandate_type
    "tipoFineMandato",  # end_mandate_type
    "provinciaNascita",  # birth_province
    "legislatura",  # legislature
]


def roman_to_int(roman):
    roman_map = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    total = 0
    prev = 0
    for char in reversed(roman):
        value = roman_map[char]
        if value < prev:
            total -= value
        else:
            total += value
        prev = value
    return total


def get_latest_legislature(context: "Context") -> int | None:
    doc = context.fetch_html(CURRENT_URL, cache_days=1)
    legislature_text = doc.findtext(".//div[@class='current_legislatura']/p/text()")
    assert legislature_text is not None, "Could not find current legislature element"
    if "legislatura corrente" not in legislature_text.lower():
        context.log.warn(
            "Cannot find current legislature in the document", url=CURRENT_URL
        )
    # Extract Roman numeral
    match = re.search(r"\b([IVXLCDM]+)\b", legislature_text)
    if not match:
        context.log.warn(
            "Could not detect Roman numeral for legislature", url=CURRENT_URL
        )
        return None
    return roman_to_int(match.group(1))


def crawl_item(context: Context, item):
    first_name = item.pop("nome").get("value")
    last_name = item.pop("cognome").get("value")
    dob = item.pop("dataNascita").get("value")

    entity = context.make("Person")
    entity.id = context.make_id(first_name, last_name, dob)
    h.apply_name(entity, first_name=first_name, last_name=last_name)
    entity.add("sourceUrl", item.pop("senatore").get("value"))
    entity.add("birthPlace", item.pop("cittaNascita").get("value"))
    entity.add("birthCountry", item.pop("nazioneNascita").get("value"))
    entity.add("gender", item.pop("sesso", {}).get("value"))
    entity.add("citizenship", "it")
    h.apply_date(entity, "birthDate", dob)

    position = h.make_position(
        context,
        name="Member of the Senate",
        wikidata_id="Q13653224",
        country="it",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        start_date=item.pop("inizioMandato").get("value"),
        end_date=item.pop("fineMandato", {}).get("value"),  # can be None
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(position)
        context.emit(entity)

    context.audit_data(item, IGNORE)


def crawl(context: Context):
    # At any given point, we only crawl the last two legislatures
    latest = get_latest_legislature(context)
    last_two = [latest - 1, latest]
    for leg in last_two:
        context.log.info(f"Crawling legislature {leg}")
        DATA["legislatura"] = str(leg)
        DATA["search[legislatura]"] = str(leg)
        path = context.fetch_resource(
            f"senatori_legislatura_{leg}.json",
            context.data_url,
            method="POST",
            data=DATA,
        )
        context.export_resource(path, JSON, title=context.SOURCE_TITLE)
        with open(path, "r") as fh:
            data = json.load(fh)
        for item in data["results"]["bindings"]:
            crawl_item(context, item)
