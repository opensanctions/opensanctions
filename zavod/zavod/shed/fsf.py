from lxml import etree
from lxml.etree import _Element as Element
from banal import as_bool
from typing import Optional
from prefixdate import parse_parts
from followthemoney import registry
from rigour.langs import iso_639_alpha3
import re

from zavod import Context, Entity
from zavod import helpers as h

# e.g. FDLR-FOCA is led by i“Lieutenant-General” Gaston Iyamuremye, alias Rumuli
#      or Victor Byiringiro and “General” Pacifique Ntawunguka, alias Omegam.
REGEX_LEADER_ALIAS = re.compile(r"led by .+ alias")

# position and title are split like this, and currently they're up to (c)
LETTER_SPLITS = ["(a)", "(b)", "(c)", "(d)", "(e)"]


def parse_country(node: Element) -> Optional[str]:
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


def parse_address(context: Context, el: Element) -> Optional[Entity]:
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


def parse_sanctions(context: Context, entity: Entity, entry: Element) -> None:
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
        source_program_key = regulation.get("programme")
        sanction = h.make_sanction(
            context,
            entity,
            program_name=source_program_key,
            # Map the source program key to the OpenSanctions program key using a lookup (e.g. BE -> BE-FOD-NAT)
            source_program_key=source_program_key,
            program_key=(
                h.lookup_sanction_program_key(context, source_program_key)
                if source_program_key
                else None
            ),
            key=url,
        )
        sanction.set("sourceUrl", url)
        sanction.add("reason", regulation.get("numberTitle"))

        # Sometimes the entity tag doesn't have a designationDate,
        # and the regulation date is the correct start date.
        # Sometimes the referenced regulation is an amendment,
        # not the original regulation introducing the entity.
        # In case both exist, use the earliest date.
        start_dates = [
            entry.get("designationDate"),
            regulation.get("entryIntoForceDate"),
        ]
        valid_start_dates = [d for d in start_dates if d is not None]
        start_date = min(valid_start_dates, default=None)
        sanction.add("startDate", start_date)

        sanction.add("listingDate", regulation.get("publicationDate"))
        entity.add("modifiedAt", regulation.get("entryIntoForceDate"))
        sanction.add("unscId", entry.get("unitedNationId"))
        sanction.add("authorityId", entry.get("euReferenceNumber"))
        context.emit(sanction)


def parse_entry(context: Context, entry: Element) -> None:
    eu_ref = entry.get("euReferenceNumber")
    if eu_ref is not None:
        entity_id = context.make_slug(eu_ref, prefix="eu-fsf")
    else:
        entity_id = context.make_slug("logical", entry.get("logicalId"))

    subject_type = entry.find("./subjectType")
    if subject_type is None:
        context.log.warning("Unknown subject type", entry=entry)
        return
    schema = context.lookup_value("subject_type", subject_type.get("code"))
    if schema is None:
        context.log.warning("Unknown subject type", type=subject_type)
        return
    schema = context.lookup_value("schema_override", entity_id, schema)
    if schema is None:
        context.log.warning("Broken schema override", entity_id=entity_id)
        return

    entity = context.make(schema)
    entity.id = entity_id
    entity.add("notes", h.clean_note(entry.findtext("./remark")))
    entity.add("topics", "sanction")
    parse_sanctions(context, entity, entry)
    # context.inspect(entry)

    for name in entry.findall("./nameAlias"):
        is_weak = not as_bool(name.get("strong"))
        remark = name.findtext("./remark")
        if remark is not None:
            # context.inspect(name)
            lremark = remark.lower()
            if "low quality" in lremark or "lo quality" in lremark:
                is_weak = True
                remark = None
            elif "ood quality" in lremark or "god quality" in lremark:
                remark = None
            elif "high quality" in lremark:
                remark = None
            elif "quality" in lremark:
                context.log.warning("Unknown quality", remark=remark)
            elif REGEX_LEADER_ALIAS.search(lremark):
                pass
            elif "alias" in lremark:
                context.log.warning("Unknown alias remark", remark=remark)
            entity.add("notes", remark, quiet=True)
        lang2 = name.get("nameLanguage")
        lang = iso_639_alpha3(lang2) if lang2 else None
        if lang is None and lang2 is not None and len(lang2):
            context.log.warning("Unknown language", lang=lang2)
            continue
        h.apply_name(
            entity,
            full=name.get("wholeName"),
            first_name=name.get("firstName"),
            middle_name=name.get("middleName"),
            last_name=name.get("lastName"),
            is_weak=is_weak,
            quiet=True,
            lang=lang,
        )
        # split "(a) Mullah, (b) Maulavi" into ["Mullah", "Maulavi"]
        titles = [
            t.strip(", ") for t in h.multi_split(name.get("title", ""), LETTER_SPLITS)
        ]
        entity.add("title", titles, quiet=True, lang=lang)
        if entity.schema.is_a("Person"):
            positions = [
                t.strip(", ")
                for t in h.multi_split(name.get("function", ""), LETTER_SPLITS)
            ]
            entity.add("position", positions, lang=lang)
        else:
            # Notes will also have LETTER_SPLITS, but there isn't really any value in splitting them
            entity.add("notes", name.get("function"), lang=lang)
        entity.add("gender", name.get("gender"), quiet=True, lang=lang)

    for node in entry.findall("./identification"):
        doc_type = node.get("identificationTypeCode")
        country = parse_country(node)
        latin_number = node.get("latinNumber")
        number = node.get("number") or latin_number
        result = context.lookup("identification_type", doc_type)
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
                for remark_node in node.findall("./remark"):
                    passport.add("summary", remark_node.text)
                context.emit(passport)

    for node in entry.findall("./address"):
        address = parse_address(context, node)
        h.apply_address(context, entity, address)

        for child in node.iterchildren():
            if child.tag in ("regulationSummary"):
                continue
            elif child.tag == "remark":
                entity.add("notes", child.text)
            elif child.tag == "contactInfo":
                res = context.lookup("contact_info", child.get("key"))
                if res is None:
                    context.log.warning("Unknown contact info", node=child)
                else:
                    if res.prop is not None:
                        values = h.multi_split(child.get("value"), [",", ";"])
                        values = [v.strip() for v in values]
                        entity.add(res.prop, values)
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
    context.emit(entity)
