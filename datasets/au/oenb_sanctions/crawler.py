import csv
import re
from datetime import datetime
from typing import Optional, Dict
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


def parse_date_time(text: str) -> Optional[datetime]:
    if not text:
        return None
    for de, number in MONTHS_DE.items():
        # Replace German month names with numbers
        text = re.sub(rf"\b{de}\b", str(number) + ".", text)
    text = text.replace(" ", "")
    date = h.parse_date(text, DATE_FORMATS)
    print(text, date)
    return date


def crawl_row(context: Context, row: Dict[str, str]):
    data = dict(row)
    full_name = data.pop("name", None)
    other_name = data.pop("other name", None)
    alias = data.pop("alias", None)
    notes = data.pop("notes", None)
    reason = data.pop("reason", None)
    birth_date = parse_date_time(data.pop("date of birth"))
    country = data.pop("country", None)
    birth_place = data.pop("place of birth", None)
    id_number = data.pop("ID card no.", None)
    source = data.pop("source", None)
    entity_type = data.pop("type", None)

    if entity_type == "Person":
        entity = context.make("Person")
        entity.id = context.make_id(full_name, birth_place)
        entity.add("name", h.split_comma_names(context, full_name))
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
        entity.add("weakAlias", alias)
        entity.add("notes", notes, lang="deu")
        entity.add("sourceUrl", source)

    # Proceed only if the entity was created
    if entity:
        entity.add("topics", "sanction")
        sanction = h.make_sanction(context, entity)
        sanction.add("reason", reason, lang="deu")
        # Emit the entities
        context.emit(entity, target=True)
        context.emit(sanction)
    # Log warnings if there are unhandled fields remaining in the dict
    context.audit_data(data)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
