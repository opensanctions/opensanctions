from lxml import etree
from lxml.etree import _Element as Element
from banal import as_bool
from prefixdate import parse_parts
from followthemoney.types import registry

from opensanctions.core import Context, Entity
from opensanctions import helpers as h
from opensanctions.util import multi_split


def parse_country(node):
    description = node.get("countryDescription")
    if description == "UNKNOWN":
        return None
    code = registry.country.clean(description)
    if code is not None:
        return code
    code = node.get("countryIso2Code")
    if code == "CS":
        return "RS"
    return code


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
        country_code=parse_country(el),
    )


def parse_sanctions(context: Context, entity: Entity, entry: Element):
    regulations = entry.findall("./regulation")
    # if len(regulations) == 0:
    #     context.log.warning(
    #         "No regulations on entity",
    #         entity=entity,
    #         regulations=len(regulations),
    #     )

    for regulation in regulations:
        url = regulation.findtext("./publicationUrl")
        assert url is not None, etree.tostring(regulation)
        sanction = h.make_sanction(context, entity, key=url)
        sanction.set("sourceUrl", url)
        sanction.add("program", regulation.get("programme"))
        sanction.add("reason", regulation.get("numberTitle"))
        sanction.add("startDate", regulation.get("entryIntoForceDate"))
        sanction.add("listingDate", regulation.get("publicationDate"))
        entity.add("createdAt", regulation.get("publicationDate"))
        sanction.add("unscId", entry.get("unitedNationId"))
        sanction.add("authorityId", entry.get("euReferenceNumber"))
        context.emit(sanction)


def parse_entry(context: Context, entry: Element):
    subject_type = entry.find("./subjectType")
    if subject_type is None:
        context.log.warning("Unknown subject type", entry=entry)
        return
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
        entity.id = context.make_slug(eu_ref, prefix="eu-fsf")
    else:
        entity.id = context.make_slug("logical", entry.get("logicalId"))
    entity.add("notes", h.clean_note(entry.findtext("./remark")))
    entity.add("topics", "sanction")
    parse_sanctions(context, entity, entry)

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
        doc_type = node.get("identificationTypeCode")
        country = parse_country(node)
        latin_number = node.get("latinNumber")
        number = node.get("number") or latin_number
        result = context.lookup("identification_type", doc_type, dataset="eu_fsf")
        if result is None:
            context.log.warning(
                "Unknown identification type",
                doc_type=doc_type,
                description=node.get("identificationTypeDescription"),
                number=number,
                country=country,
            )
            continue
        if result.prop is not None:
            entity.add(result.prop, number, quiet=True)
            entity.add(result.prop, latin_number, quiet=True)
            entity.add("country", country, quiet=True)
        if result.schema is not None:
            passport = h.make_identification(
                context,
                entity,
                number=number,
                doc_type=node.get("identificationTypeDescription"),
                authority=node.get("issuedBy"),
                start_date=node.get("issueDate"),
                country=country,
                key=node.get("logicalId"),
            )
            if passport is not None:
                passport.add("number", latin_number)
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
                    values = multi_split(child.get("value"), [",", ";"])
                    values = [v.strip() for v in values]
                    entity.add(prop, values)
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
        entity.add("nationality", parse_country(node), quiet=True)

    # context.inspect(entry)
    context.emit(entity, target=True)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = h.remove_namespace(doc)
    for entry in doc.findall(".//sanctionEntity"):
        parse_entry(context, entry)
