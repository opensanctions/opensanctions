from banal import as_bool
from prefixdate import parse_parts

from opensanctions.core import Context
from opensanctions import helpers as h


def parse_address(context: Context, el):
    country = el.get("countryDescription")
    if country == "UNKNOWN":
        country = None
    # context.log.info("Addrr", el=el)
    return h.make_address(
        context,
        street=el.get("street"),
        po_box=el.get("poBox"),
        city=el.get("city"),
        place=el.get("place"),
        postal_code=el.get("zipCode"),
        region=el.get("region"),
        country=country,
        country_code=el.get("countryIso2Code"),
    )


def parse_entry(context: Context, entry):
    subject_type = entry.find("./subjectType")
    schema = context.lookup_value(
        "subject_type",
        subject_type.get("code"),
        dataset="eu_fsf",
    )
    if schema is None:
        context.log.warning("Unknown subject type", type=subject_type)
        return

    entity = context.make(schema)
    eu_ref = entry.get("euReferenceNumber")
    if eu_ref is not None:
        entity.id = context.make_slug(eu_ref, dataset="eu_fsf")
    else:
        entity.id = context.make_slug("logical", entry.get("logicalId"))
    entity.add("notes", h.clean_note(entry.findtext("./remark")))
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    regulations = entry.findall("./regulation")
    if len(regulations) != 1:
        context.log.warning("Multiple regulations on entity", entity=entity)
    regulation = regulations[0]
    sanction.set("sourceUrl", regulation.findtext("./publicationUrl"))
    sanction.add("program", regulation.get("programme"))
    sanction.add("reason", regulation.get("numberTitle"))
    sanction.add("startDate", regulation.get("entryIntoForceDate"))
    sanction.add("listingDate", regulation.get("publicationDate"))
    entity.add("createdAt", regulation.get("publicationDate"))
    sanction.add("unscId", entry.get("unitedNationId"))
    sanction.add("authorityId", entry.get("euReferenceNumber"))

    for name in entry.findall("./nameAlias"):
        is_weak = not as_bool(name.get("strong"))
        h.apply_name(
            entity,
            full=name.get("wholeName"),
            first_name=name.get("firstName"),
            middle_name=name.get("middleName"),
            last_name=name.get("lastName"),
            is_weak=is_weak,
            quiet=True,
        )
        entity.add("title", name.get("title"), quiet=True)
        entity.add("position", name.get("function"), quiet=True)
        entity.add("gender", name.get("gender"), quiet=True)

    for node in entry.findall("./identification"):
        type = node.get("identificationTypeCode")
        schema = "Passport" if type == "passport" else "Identification"
        passport = context.make(schema)
        passport.id = context.make_id("ID", entity.id, node.get("logicalId"))
        passport.add("holder", entity)
        passport.add("authority", node.get("issuedBy"))
        passport.add("type", node.get("identificationTypeDescription"))
        passport.add("number", node.get("number"))
        passport.add("number", node.get("latinNumber"))
        passport.add("startDate", node.get("issueDate"))
        passport.add("startDate", node.get("issueDate"))
        passport.add("country", node.get("countryIso2Code"))
        passport.add("country", node.get("countryDescription"))
        for remark in node.findall("./remark"):
            passport.add("summary", remark.text)
        context.emit(passport)

    for node in entry.findall("./address"):
        address = parse_address(context, node)
        h.apply_address(context, entity, address)

        for child in node.getchildren():
            if child.tag in ("regulationSummary"):
                continue
            elif child.tag == "remark":
                entity.add("notes", child.text)
            elif child.tag == "contactInfo":
                prop = context.lookup_value(
                    "contact_info",
                    child.get("key"),
                    dataset="eu_fsf",
                )
                if prop is None:
                    context.log.warning("Unknown contact info", node=child)
                else:
                    entity.add(prop, child.get("value"))
            else:
                context.log.warning("Unknown address component", node=child)

    for birth in entry.findall("./birthdate"):
        partialBirth = parse_parts(
            birth.get("year"), birth.get("month"), birth.get("day")
        )
        entity.add("birthDate", birth.get("birthdate"))
        entity.add("birthDate", partialBirth)
        address = parse_address(context, birth)
        if address is not None:
            entity.add("birthPlace", address.get("full"))
            entity.add("country", address.get("country"))

    for node in entry.findall("./citizenship"):
        entity.add("nationality", node.get("countryIso2Code"), quiet=True)
        entity.add("nationality", node.get("countryDescription"), quiet=True)

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = h.remove_namespace(doc)
    for entry in doc.findall(".//sanctionEntity"):
        parse_entry(context, entry)
