import re
from typing import Generator, Optional
from datetime import datetime
from followthemoney.types.identifier import IdentifierType

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.helpers.xml import ElementOrTree
from zavod.shed.zyte_api import fetch_resource
from normality import collapse_spaces


REGEX_ID_NUMBER = re.compile(r"\w?[\d-]*\d{6,}[\d-]*")
SPLITS = [";", "i)", "ii)", "iii)", "iv)", "v)", "vi)", "vii)", "viii)", "ix)", "x)"]
DATES_SPLITS = [
    ";",
    "Approximately",
    "Circa",
    "Between",
    "circa",
    "approximately",
    "or ",
    "born in ",
    " in ",
]


def clean_id(entity: Entity, text: Optional[str]) -> Generator[str, None, None]:
    if text is None:
        return []
    for substring in text.split(";"):
        if len(substring) > IdentifierType.max_length:
            entity.add("notes", substring)
        yield from REGEX_ID_NUMBER.findall(substring)


def parse_entry(context: Context, entry: ElementOrTree) -> None:
    entity = context.make("LegalEntity")
    if entry.findtext("./type-entry") == "2":
        entity = context.make("Person")
    entry_id = entry.findtext("number-entry")
    if entry_id == "1460":
        entity = context.make("LegalEntity")
    entity.id = context.make_slug(entry_id)

    sanction = h.make_sanction(context, entity)
    sanction.add("program", entry.findtext("./program-entry"))
    date_entry = entry.findtext("./date-entry")
    if date_entry:
        date = datetime.strptime(date_entry, "%Y%m%d")
        entity.add("createdAt", date.date())
        sanction.add("listingDate", date.date())
        sanction.add("startDate", date.date())

    for aka in entry.findall("./aka-list"):
        h.apply_name(
            entity,
            name1=aka.findtext("./aka-name1"),
            name2=aka.findtext("./aka-name2"),
            name3=aka.findtext("./aka-name3"),
            tail_name=aka.findtext("./aka-name4"),
            alias=aka.findtext("type-aka") != "N",
            is_weak=aka.findtext("./quality-aka") == "2",
            quiet=True,
        )

    for node in entry.findall("./title-list"):
        entity.add("title", node.text, quiet=True)

    for doc in entry.findall("./document-list"):
        for number in clean_id(entity, doc.findtext("./document-id")):
            passport = h.make_identification(
                context,
                entity,
                number=number,
                summary=doc.findtext("./document-reg"),
                country=doc.findtext("./document-country"),
                passport=True,
            )
            if passport is not None:
                context.emit(passport)

    for doc in entry.findall("./id-number-list"):
        entity.add("idNumber", list(clean_id(entity, doc.text)))

    for node in entry.findall("./address-list"):
        entity.add("address", h.multi_split(node.findtext("./address"), SPLITS))

    for pob in entry.findall("./place-of-birth-list"):
        for p in h.multi_split(pob.text, ";"):
            entity.add_cast("Person", "birthPlace", collapse_spaces(p))

    for dob in entry.findall("./date-of-birth-list"):
        if dob.text is not None:
            entity.add_schema("Person")
            dates = h.multi_split(dob.text, DATES_SPLITS)
            for date in dates:
                h.apply_date(entity, "birthDate", date)

    for nat in entry.findall("./nationality-list"):
        for country in h.multi_split(nat.text, [";", ","]):
            country_ = h.remove_bracketed(country)
            entity.add("nationality", country_, quiet=True)

    entity.add("topics", "sanction")
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    _, _, _, path = fetch_resource(
        context,
        "source.xml",
        context.data_url,
        expected_media_type="text/xml",
        geolocation="pl",
    )
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    for entry in doc.findall(".//acount-list"):
        parse_entry(context, entry)
