from datetime import datetime

from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import jointext, remove_bracketed, multi_split

FORMATS = ["%d %b %Y", "%d %B %Y", "%Y", "%b %Y", "%B %Y"]


def parse_date(date):
    if date is None:
        return
    date = date.replace(".", "")
    if ";" in date:
        date, _ = date.split(";", 1)
    date = date.strip()
    return h.parse_date(date, FORMATS)


async def parse_entry(context: Context, entry):
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
        entity.context["created_at"] = date.isoformat()
        sanction.add("startDate", date.date())

    for aka in entry.findall("./aka-list"):
        first_name = aka.findtext("./aka-name1")
        entity.add("firstName", first_name, quiet=True)
        second_name = aka.findtext("./aka-name2")
        entity.add("secondName", second_name, quiet=True)
        third_name = aka.findtext("./aka-name3")
        entity.add("middleName", third_name, quiet=True)
        last_name = aka.findtext("./aka-name4")
        entity.add("lastName", last_name, quiet=True)
        name = jointext(first_name, second_name, third_name, last_name)
        if aka.findtext("type-aka") == "N":
            entity.add("name", name)
        else:
            if aka.findtext("./quality-aka") == "2":
                entity.add("weakAlias", name)
            else:
                entity.add("alias", name)

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
        await context.emit(passport)

    for doc in entry.findall("./id-number-list"):
        entity.add("idNumber", doc.text)

    for node in entry.findall("./address-list"):
        address = h.make_address(context, full=node.findtext("./address"))
        await h.apply_address(context, entity, address)

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
    await context.emit(entity, target=True)
    await context.emit(sanction)


async def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
    await context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    for entry in doc.findall(".//acount-list"):
        await parse_entry(context, entry)
