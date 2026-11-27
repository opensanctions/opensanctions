import re
from collections.abc import Generator
from datetime import datetime
from followthemoney.types.identifier import IdentifierType

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.util import Element
from zavod.extract.zyte_api import fetch_resource
from zavod.shed.un_sc import apply_un_name_list
from normality import collapse_spaces


REGEX_ID_NUMBER = re.compile(r"\w?[\d-]*\d{6,}[\d-]*")
ROMAN_NUMERALS = ["i)", "ii)", "iii)", "iv)", "v)", "vi)", "vii)", "viii)", "ix)", "x)"]
# h.multi_split() applies splits sequentially
# when it splits on "i)" before "ii)", any occurrence of "ii)" gets split into "i" + "" because "ii)" contains the substring "i)"
# "...Afghanistan ii) Ansari Market..." becomes ["...Afghanistan i", " Ansari Market..."]
SPLITS = [";"] + sorted(ROMAN_NUMERALS, key=len, reverse=True)
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
HANDLED_TAGS = {
    "number-entry",
    "type-entry",
    "program-entry",
    "date-entry",
    "aka-list",
    "title-list",
    "document-list",
    "id-number-list",
    "address-list",
    "place-of-birth-list",
    "date-of-birth-list",
    "nationality-list",
    "citizenchip-list",
    "designation-list",
    "working-list",
    "comments",
    "noted-on-conviction",
    "noted-on-prosecution",
    # The amendment history of the record, and record-level metadata.
    "comments-record",
    "type-record",
}


def clean_id(entity: Entity, text: str | None) -> Generator[str, None, None]:
    if text is None:
        return
    for substring in text.split(";"):
        if len(substring) > IdentifierType.max_length:
            entity.add("notes", substring)
        yield from REGEX_ID_NUMBER.findall(substring)


def parse_entry(context: Context, entry: Element) -> None:
    for child in entry.iterchildren():
        if child.tag not in HANDLED_TAGS:
            context.log.warning("Unhandled field", tag=child.tag, text=child.text)

    entity = context.make("LegalEntity")
    if entry.findtext("./type-entry") == "2":
        entity = context.make("Person")
    entry_id = entry.findtext("number-entry")
    entity.id = context.make_slug(entry_id)

    sanction = h.make_sanction(context, entity)
    sanction.add("program", entry.findtext("./program-entry"))
    # SFMS moves date-entry to the day of each amendment to the record, so it
    # doesn't date the listing.
    date_entry = entry.findtext("./date-entry")
    if date_entry:
        date = datetime.strptime(date_entry, "%Y%m%d")
        sanction.add("modifiedAt", date.date())

    for aka in entry.findall("./aka-list"):
        aka_category = aka.findtext("category-aka")
        names = [
            name
            for name in [
                aka.findtext("./aka-name1"),
                aka.findtext("./aka-name2"),
                aka.findtext("./aka-name3"),
                aka.findtext("./aka-name4"),
            ]
            if name is not None and name != ""
        ]
        if aka_category is None:
            # The names seem to be copied from the UN list for the 'N' type,
            # so use the same semantics
            # The NU type is a cyrillic variant of the N type, sometimes in
            # Ukrainian, sometimes in Russian, so no language tag.
            apply_un_name_list(context, entity, names)
        elif aka_category in ("aka", "fka"):
            # make_name (which is just a fancy wrapper around " ".join) to generate the full name.
            name_args = {f"name{i + 1}": name for i, name in enumerate(names)}
            joined_name = h.make_name(**name_args)

            if aka_category == "aka":
                is_weak = aka.findtext("./quality-aka") == "2"
                entity.add("weakAlias" if is_weak else "alias", joined_name)
            elif aka_category == "fka":
                entity.add("previousName", joined_name)
        else:
            context.log.warning("Unknown aka category", category=aka_category)

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
                start_date=doc.findtext("./document-date"),
                passport=True,
            )
            if passport is not None:
                context.emit(passport)

    for doc in entry.findall("./id-number-list"):
        entity.add("idNumber", list(clean_id(entity, doc.text)))

    for node in entry.findall("./address-list"):
        # Newer records move the city and country out of the address text.
        city = node.findtext("./city")
        state = node.findtext("./state-province")
        country = node.findtext("./country")
        streets: list[str | None] = []
        streets.extend(h.multi_split(node.findtext("./address"), SPLITS))
        for street in streets or [None]:
            address = h.format_address(
                street=street, city=city, state=state, country=country
            )
            entity.add("address", address)

    for pob in entry.findall("./place-of-birth-list"):
        for p in h.multi_split(pob.text, ";"):
            entity.add_cast("Person", "birthPlace", collapse_spaces(p))

    for dob in entry.findall("./date-of-birth-list"):
        if dob.text is not None:
            entity.add_schema("Person")
            dates = h.multi_split(dob.text, DATES_SPLITS)
            for dob_date in dates:
                h.apply_date(entity, "birthDate", dob_date)

    for nat in entry.findall("./nationality-list"):
        for country in h.multi_split(nat.text, [";", ","]):
            country_ = h.remove_bracketed(country)
            entity.add("nationality", country_, quiet=True)

    for node in entry.findall("./citizenchip-list"):
        for country in h.multi_split(node.text, [";", ","]):
            entity.add("citizenship", h.remove_bracketed(country))

    for tag in ("./designation-list", "./working-list"):
        for node in entry.findall(tag):
            entity.add("position", node.text)

    for tag in ("./comments", "./noted-on-conviction", "./noted-on-prosecution"):
        entity.add("notes", h.clean_note(entry.findtext(tag)))

    entity.add("topics", "sanction")
    h.finalize_sanction(
        context, sanction, key=entry_id, checksum=h.xml_node_sorted_hash(entry)
    )
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
