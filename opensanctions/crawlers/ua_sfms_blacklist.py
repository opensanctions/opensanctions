from datetime import datetime

from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import remove_bracketed, multi_split

FORMATS = ["%d %b %Y", "%d %B %Y", "%Y", "%b %Y", "%B %Y"]


def parse_date(date):
    if date is None:
        return
    date = date.replace(".", "")
    if ";" in date:
        date, _ = date.split(";", 1)
    date = date.strip()
    return h.parse_date(date, FORMATS)


def parse_entry(context: Context, entry):
    entity = context.make("LegalEntity")
    if entry.findtext("./type-entry") == "2":
        entity = context.make("Person")
    entry_id = entry.findtext("number-entry")
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
        reg = doc.findtext("./document-reg")
        number = doc.findtext("./document-id")
        country = doc.findtext("./document-country")
        passport = context.make("Passport")
        passport.id = context.make_id("Passport", entity.id, reg, number, country)
        passport.add("holder", entity)
        passport.add("passportNumber", number)
        passport.add("summary", reg)
        passport.add("country", country)
        context.emit(passport)

    for doc in entry.findall("./id-number-list"):
        entity.add("idNumber", doc.text)

    for node in entry.findall("./address-list"):
        address = h.make_address(context, full=node.findtext("./address"))
        h.apply_address(context, entity, address)

    for pob in entry.findall("./place-of-birth-list"):
        entity.add_cast("Person", "birthPlace", pob.text)

    for dob in entry.findall("./date-of-birth-list"):
        date = parse_date(dob.text)
        entity.add_cast("Person", "birthDate", date)

    for nat in entry.findall("./nationality-list"):
        for country in multi_split(nat.text, [";", ","]):
            country = remove_bracketed(country)
            entity.add("nationality", country, quiet=True)

    entity.add("topics", "sanction")
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    for entry in doc.findall(".//acount-list"):
        parse_entry(context, entry)
