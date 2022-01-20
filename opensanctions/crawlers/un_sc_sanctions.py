from normality import collapse_spaces

from opensanctions.core import Context
from opensanctions import helpers as h


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


def parse_address(context: Context, node):
    return h.make_address(
        context,
        remarks=node.findtext("./NOTE"),
        street=node.findtext("./STREET"),
        city=node.findtext("./CITY"),
        region=node.findtext("./STATE_PROVINCE"),
        postal_code=node.findtext("./ZIP_CODE"),
        country=node.findtext("./COUNTRY"),
    )


def parse_entity(context: Context, node):
    entity = context.make("LegalEntity")
    sanction = parse_common(context, entity, node)
    entity.add("alias", node.findtext("./FIRST_NAME"))

    for alias in node.findall("./ENTITY_ALIAS"):
        parse_alias(entity, alias)

    for addr in node.findall("./ENTITY_ADDRESS"):
        h.apply_address(context, entity, parse_address(context, addr))

    context.emit(entity, target=True, unique=True)
    context.emit(sanction)


def parse_individual(context: Context, node):
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
        h.apply_address(context, person, parse_address(context, addr))

    for doc in node.findall("./INDIVIDUAL_DOCUMENT"):
        passport = context.make("Passport")
        number = doc.findtext("./NUMBER")
        date = doc.findtext("./DATE_OF_ISSUE")
        type_ = doc.findtext("./TYPE_OF_DOCUMENT")
        if number is None and date is None and type_ is None:
            continue
        passport.id = context.make_id(person.id, number, date, type_)
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
        address = parse_address(context, pob)
        if address is not None:
            person.add("birthPlace", address.get("full"))
            person.add("country", address.get("country"))

    context.emit(person, target=True, unique=True)
    context.emit(sanction)


def parse_common(context: Context, entity, node):
    entity.id = context.make_slug(node.findtext("./DATAID"))
    name = node.findtext("./NAME_ORIGINAL_SCRIPT")
    name = name or node.findtext("./FIRST_NAME")
    entity.add("name", name)
    entity.add("notes", node.findtext("./COMMENTS1"))
    entity.add("topics", "sanction")
    updated_at = values(node.find("./LAST_DAY_UPDATED"))
    entity.add("modifiedAt", updated_at)
    listed_on = node.findtext("./LISTED_ON")
    entity.add("createdAt", listed_on)

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", listed_on)
    sanction.add("modifiedAt", values(node.find("./LAST_DAY_UPDATED")))
    sanction.add("program", node.findtext("./UN_LIST_TYPE"))
    sanction.add("recordId", node.findtext("./REFERENCE_NUMBER"))
    return sanction


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)

    for node in doc.findall(".//INDIVIDUAL"):
        parse_individual(context, node)

    for node in doc.findall(".//ENTITY"):
        parse_entity(context, node)
