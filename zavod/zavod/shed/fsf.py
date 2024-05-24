from lxml import etree
from lxml.etree import _Element as Element
from banal import as_bool
from typing import Optional
from prefixdate import parse_parts
from followthemoney.types import registry
from rigour.langs import iso_639_alpha3
import re

from zavod import Context, Entity
from zavod import helpers as h

# e.g. FDLR-FOCA is led by i“Lieutenant-General” Gaston Iyamuremye, alias Rumuli
#      or Victor Byiringiro and “General” Pacifique Ntawunguka, alias Omegam.
REGEX_LEADER_ALIAS = re.compile(r"led by .+ alias")


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


def parse_entry(context: Context, entry: Element) -> None:
    subject_type = entry.find("./subjectType")
    if subject_type is None:
        context.log.warning("Unknown subject type", entry=entry)
        return
    schema = context.lookup_value("subject_type", subject_type.get("code"))
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
        entity.add("title", name.get("title"), quiet=True, lang=lang)
        entity.add("position", name.get("function"), quiet=True, lang=lang)
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
    context.emit(entity, target=True)
