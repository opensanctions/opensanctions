import csv
from typing import Dict
from zavod import Context, helpers as h
import re
from typing import List


DATE_FORMATS = ["%d.%m.%Y", "%d. %m %Y"]
MONTHS_EN = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}


def parse_date_time(text: str) -> List[str]:
    if not text:
        return None
    for de, number in MONTHS_EN.items():
        # Replace German month names with numbers
        text = re.sub(rf"\b{de}\b", str(number) + ".", text)
    text = text.replace(" ", "")
    date = h.parse_date(text, DATE_FORMATS)
    return date


def crawl_row(context: Context, row: Dict[str, str]):
    full_name = row.pop("name")
    other_name = row.pop("other name")
    birth_date = parse_date_time(row.pop("date of birth"))  # still to adjust
    birth_place = row.pop("place of birth")
    nationality = row.pop("nationality")
    passport_number = row.pop("passport No.")
    position = row.pop("position")
    residence = row.pop("residence")
    domicile = row.pop("domicile")
    fiscal_code = row.pop("fiscal code")
    phone_number = row.pop("phone number")
    address = row.pop("address")
    address_1 = row.pop("address line 1")
    address_2 = row.pop("address line 2")
    address_3 = row.pop("address line 3")
    city = row.pop("city")
    region = row.pop("region (province)")
    country = row.pop("country")
    entity_type = row.pop("type")

    entity = None
    if entity_type == "Person":
        entity = context.make("Person")
        entity.id = context.make_id(full_name, other_name, passport_number)
        entity.add("name", full_name)
        entity.add("alias", other_name)
        entity.add("birthDate", birth_date)
        entity.add("birthPlace", birth_place)
        entity.add("nationality", nationality)
        entity.add("passportNumber", passport_number)
        entity.add("address", residence)
        entity.add("address", domicile)
        entity.add("taxNumber", fiscal_code)
        entity.add("phone", phone_number)
        entity.add("topics", "sanction")
        entity.add("country", "ro")
        entity.add("position", position)
    elif entity_type == "Organization":
        entity = context.make("Organization")
        entity.id = context.make_id(full_name, address)
        entity.add("name", full_name)
        entity.add("address", address)
        entity.add("address", address_1)
        entity.add("address", address_2)
        entity.add("address", address_3)
        entity.add("city", city)
        entity.add("jurisdiction", country)
        entity.add("region", region)
        entity.add("country", country)
    else:
        context.log.warning("Unhandled entity type", type=entity_type)

    if entity:
        entity.add("topics", "sanction")
        sanction = h.make_sanction(context, entity)
        # Emit the entities
        context.emit(entity, target=True)
        context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
