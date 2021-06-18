from pprint import pprint  # noqa
from normality import collapse_spaces

from opensanctions.util import jointext


def values(node):
    if node is None:
        return []
    return [c.text for c in node.findall("./VALUE")]


def parse_alias(entity, node):
    names = node.findtext("./ALIAS_NAME")
    quality = node.findtext("./QUALITY")
    if names is None:
        return

    for name in names.split("; "):
        name = collapse_spaces(name)
        if not len(name):
            continue

        if quality == "Low":
            entity.add("weakAlias", name)
        elif quality == "Good":
            entity.add("alias", name)
        elif quality == "a.k.a.":
            entity.add("alias", name)
        elif quality == "f.k.a.":
            entity.add("previousName", name)


def parse_address(entity, addr):
    text = addr.xpath("string()").strip()
    if not len(text):
        return
    country = addr.findtext("./COUNTRY")
    address = jointext(
        addr.findtext("./NOTE"),
        addr.findtext("./STREET"),
        addr.findtext("./CITY"),
        addr.findtext("./STATE_PROVINCE"),
        country,
        sep=", ",
    )
    entity.add("address", address)
    entity.add("country", country)


def parse_entity(context, node):
    entity = context.make("LegalEntity")
    sanction = parse_common(context, entity, node)
    entity.add("alias", node.findtext("./FIRST_NAME"))

    for alias in node.findall("./ENTITY_ALIAS"):
        parse_alias(entity, alias)

    for addr in node.findall("./ENTITY_ADDRESS"):
        parse_address(entity, addr)

    context.emit(entity, target=True, unique=True)
    context.emit(sanction)


def parse_individual(context, node):
    person = context.make("Person")
    sanction = parse_common(context, person, node)
    person.add("title", values(node.find("./TITLE")))
    person.add("firstName", node.findtext("./FIRST_NAME"))
    person.add("secondName", node.findtext("./SECOND_NAME"))
    person.add("middleName", node.findtext("./THIRD_NAME"))
    person.add("position", values(node.find("./DESIGNATION")))

    for alias in node.findall("./INDIVIDUAL_ALIAS"):
        parse_alias(person, alias)

    for addr in node.findall("./INDIVIDUAL_ADDRESS"):
        parse_address(person, addr)

    for doc in node.findall("./INDIVIDUAL_DOCUMENT"):
        passport = context.make("Passport")
        number = doc.findtext("./NUMBER")
        date = doc.findtext("./DATE_OF_ISSUE")
        type_ = doc.findtext("./TYPE_OF_DOCUMENT")
        if number is None and date is None and type_ is None:
            continue
        passport.make_id(person.id, number, date, type_)
        passport.add("holder", person)
        passport.add("passportNumber", number)
        passport.add("startDate", date)
        passport.add("type", type_)
        passport.add("type", doc.findtext("./TYPE_OF_DOCUMENT2"))
        passport.add("summary", doc.findtext("./NOTE"))
        country = doc.findtext("./COUNTRY_OF_ISSUE")
        country = country or doc.findtext("./ISSUING_COUNTRY")
        passport.add("country", country)
        context.emit(passport)

    for nat in node.findall("./NATIONALITY/VALUE"):
        person.add("nationality", nat.text)

    for dob in node.findall("./INDIVIDUAL_DATE_OF_BIRTH"):
        date = dob.findtext("./DATE") or dob.findtext("./YEAR")
        person.add("birthDate", date)

    for pob in node.findall("./INDIVIDUAL_PLACE_OF_BIRTH"):
        person.add("country", pob.findtext("./COUNTRY"))
        place = jointext(
            pob.findtext("./CITY"),
            pob.findtext("./STATE_PROVINCE"),
            pob.findtext("./COUNTRY"),
            sep=", ",
        )
        person.add("birthPlace", place)

    context.emit(person)
    context.emit(sanction)


def parse_common(context, entity, node):
    entity.make_slug(node.findtext("./DATAID"))
    name = node.findtext("./NAME_ORIGINAL_SCRIPT")
    name = name or node.findtext("./FIRST_NAME")
    entity.add("name", name)
    entity.add("description", node.findtext("./COMMENTS1"))
    updated_at = values(node.find("./LAST_DAY_UPDATED"))
    if len(updated_at):
        entity.add("modifiedAt", updated_at)
        entity.context["updated_at"] = max(updated_at)
    listed_on = node.findtext("./LISTED_ON")
    if listed_on is not None:
        entity.context["created_at"] = listed_on

    sanction = context.make("Sanction")
    sanction.make_id(entity.id)
    sanction.add("entity", entity)
    sanction.add("authority", "United Nations Security Council")
    sanction.add("startDate", listed_on)
    sanction.add("modifiedAt", values(node.find("./LAST_DAY_UPDATED")))

    program = "%s (%s)" % (
        node.findtext("./UN_LIST_TYPE").strip(),
        node.findtext("./REFERENCE_NUMBER").strip(),
    )
    sanction.add("program", program)
    return sanction


def crawl(context):
    context.fetch_resource("source.xml", context.dataset.data.url)
    doc = context.parse_resource_xml("source.xml")

    for node in doc.findall(".//INDIVIDUAL"):
        parse_individual(context, node)

    for node in doc.findall(".//ENTITY"):
        parse_entity(context, node)
