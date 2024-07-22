import csv
import re
from typing import Dict, List
import zavod.helpers as h
from zavod import Context

DATE_FORMATS = ["%d.%m.%Y", "%d. %m %Y"]
MONTHS_DE = {
    "Januar": 1,
    "Februar": 2,
    "März": 3,
    "April": 4,
    "Mai": 5,
    "Juni": 6,
    "Juli": 7,
    "August": 8,
    "September": 9,
    "Oktober": 10,
    "November": 11,
    "Dezember": 12,
}


def parse_date_time(text: str) -> List[str]:
    if not text:
        return None
    for de, number in MONTHS_DE.items():
        # Replace German month names with numbers
        text = re.sub(rf"\b{de}\b", str(number) + ".", text)
    text = text.replace(" ", "")
    date = h.parse_date(text, DATE_FORMATS)
    return date


def crawl_row(context: Context, row: Dict[str, str]):
    full_name = row.pop("name")
    other_name = row.pop("other name")
    alias = row.pop("alias")
    notes = row.pop("notes")
    reason = row.pop("reason")
    birth_date = parse_date_time(row.pop("date of birth"))
    country = row.pop("country")
    birth_place = row.pop("place of birth")
    id_number = row.pop("ID card no.")
    source = row.pop("source")
    entity_type = row.pop("type")

    entity = None
    if entity_type == "Person":
        entity = context.make("Person")
        entity.id = context.make_id(full_name, birth_place, birth_date)
        entity.add("name", full_name)  # still need to split it
        entity.add("alias", other_name)
        entity.add("birthDate", birth_date)
        entity.add("birthPlace", birth_place, lang="spa")
        entity.add("country", country, lang="deu")
        entity.add("idNumber", id_number)
        entity.add("sourceUrl", source)
    elif entity_type == "Organization":
        entity = context.make("Organization")
        entity.id = context.make_id(full_name, source)
        entity.add("name", full_name)
        entity.add("alias", other_name)
        entity.add("alias", alias)
        entity.add("notes", notes, lang="deu")
        entity.add("sourceUrl", source)
    else:
        context.log.warning("Unhandled entity type", type=entity_type)

    # Proceed only if the entity was created
    if entity:
        entity.add("topics", "sanction")
        sanction = h.make_sanction(context, entity)
        sanction.add("reason", reason, lang="deu")
        # Emit the entities
        context.emit(entity, target=True)
        context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
