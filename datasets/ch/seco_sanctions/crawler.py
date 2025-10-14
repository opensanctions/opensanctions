import re
from datetime import datetime
from itertools import product
from typing import Dict, List, Literal, Optional, Tuple

from followthemoney.types import registry
from followthemoney.util import join_text
from lxml.etree import _Element as Element
from prefixdate import parse_parts
from pydantic import BaseModel, Field
from rigour.mime.types import PLAIN
from zavod.shed.gpt import run_typed_text_prompt
from zavod.stateful.review import (
    TextSourceValue,
    assert_all_accepted,
    review_extraction,
)

from zavod import Context, Entity
from zavod import helpers as h

# TODO: sanctions-program full parse
MayStr = Optional[str]

SKIP_OLD = {"41406"}
NAME_QUALITY_WEAK: Dict[MayStr, bool] = {"good": False, "low": True}
NAME_TYPE: Dict[MayStr, str] = {
    "primary-name": "name",
    "alias": "alias",
    "formerly-known-as": "previousName",
}
NAME_PARTS: Dict[MayStr, MayStr] = {
    "title": "title",
    "given-name": "firstName",
    "further-given-name": "firstName",
    "father-name": "fatherName",
    "grand-father-name": "fatherName",
    "family-name": "lastName",
    "maiden-name": "lastName",
    "suffix": "nameSuffix",
    "tribal-name": "weakAlias",
    "whole-name": None,
    "other": None,
}
# Some metadata is dirty text in <other-information> tags
# TODO: take in charge multiple values
REGEX_WEBSITE = re.compile(r"Website ?: ((https?:|www\.)\S*)")
REGEX_EMAIL = re.compile(
    r"E-?mail( address)? ?: ([A-Za-z0-9._-]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+)"
)
REGEX_PHONE = re.compile(r"(Tel\.|Telephone)( number)? ?: (\+?[0-9- ()]+)")
REGEX_INN = re.compile(r"Taxpayer [Ii]dentification [Nn]umber ?: (\d+)\.?")
REGEX_REGNUM = re.compile(
    r"(ОГРН/main )?([Ss]tate |Business )?[Rr]egistration number ?: (\d+)\.?"
)
REGEX_TAX = re.compile(r"Tax [Rr]egistration [Nn]umber ?: (\d+)\.?")
REGEX_IMO = re.compile(r"IMO [Nn]umber ?: (\d+)\.?")

PropertyName = Literal[
    "email",
    "phone",
    "website",
    "innCode",
    "ogrnCode",
    "kppCode",
    "taxNumber",
    "imoNumber",
    "okpoCode",
    "passportNumber",
    "uscCode",
    "swiftBic",
    "registrationNumber",
    "idNumber",
    "incorporationDate",
    "dissolutionDate",
    "address",
    "country",
    "position",
    "birthPlace",
    "birthDate",
    "gender",
    "legalForm",
]


class SimpleValue(BaseModel):
    property: PropertyName
    value: str


class RelatedEntity(BaseModel):
    relationship_schema: Literal["Family", "UnknownLink"]
    related_entity_name: List[str]
    relationship: Optional[str] = None


class OtherInfo(BaseModel):
    simple_values: List[SimpleValue] = []
    related_entities: List[RelatedEntity] = []
    include_in_notes: bool = Field(
        description="If True, the original value will be included in notes.",
        default=False,
    )


CRAWLER_VERSION = 1
# gpt-4o keeps extracting dissolution date from
# > Travel ban according to article 3 paragraph 1 and financial sanctions
# > according to article 1 do not apply until 15 March 2016.
# It also keenly creates lots of entries no matter how many ways I tell it not to.
# gpt-4.1-mini was hallucinating values.
# gpt-4o-mini was filling in Unknown for props not in the data.
# gpt-5-mini is super slow as at 2025-09-17 but the quality of values seem very good.
LLM_VERSION = "gpt-4o-mini"
PROMPT = """
The attached text is a string from the other-information field of an international
financial sanctions entry about an entity type of {schema}.

We will extract simple values and associated or related persons, companies,
organisations, or legal entities of unclear type.

Include ONLY the values that are present in the text.
NEVER infer, assume, or generate values that are not directly stated in the source text.

If we don't extract all the information in the text to structured data, set `include_in_notes` to `true`.

Extract simple values representing the properties described below and return in
the simple_values array.
In most cases, a string will correspond to zero or one value for one property,
but in some cases it may contain more.
Only include entries for properties applicable to the data.
NEVER make SimpleValue entries with empty/blank/unknown/not applicable values, or make placeholder entries.
Include multiple values as distinct entries in the simple_values array rather than
a single entry with comma-separated values.

SimpleValue properties:

For dates, include only as much precision as is in the data - just year or YYYY-MM is fine.

Only when indicated to be the following kind of identifier:
  - uscCode: Unified Social Credit
  - innCode: INN number
  - ogrnCode: OGRN number
  - kppCode: KPP number
  - imoNumber: IMO number for a vessel or company
  - swiftBic: Swift or BIC identifier

Only when indicated to be this kind of identifier and not one of the above more specific types:
  - taxNumber: Tax number
  - registrationNumber: Company registration number
  - idNumber: personal identification numbers

Other properties:
  - email: For email addresses listed in the text.
  - phone: For phone numbers listed in the text.
  - website: For website URLs listed in the text.
  - incorporationDate and dissolutionDate are ONLY for company registration or dissolution.
    Do NOT use these for other dates like travel ban periods.
  - legalForm: e.g. Joint Stock Company - precisely as written in the text.
  - address: include whatever level of geographic detail is available, even if it's just
    a city and/or state. For example, if a city and state is included in parentheses
    like in "Company X (Moscow, Russia)" extract "Moscow, Russia" as the address.
  - country: Any country mentioned in the text associated with the subject.

  Properties only to be used when the subject entity is a Person:
    - birthPlace: Place of birth, sometimes written as POB.
    - birthDate: Date of birth of a Person subject, sometimes written as DOB.
      If a range is given, create one entry for the start, and one for the end.
    - position:
      - Include positions prefixed with "Former", including the prefix.
      - Include positions like rank and committee membership.
      - Don't include detail about the entity where the position is held beyond
        its name and geographic scope or jurisdiction
      - Do include the company name in the position if it is simply part of the role,
        e.g. "Managing Director of OOO Bergia Group"

  Extract related entities and return in the related_entities array.
  This is e.g. for named family members of the subject if the subject is a Person.
  Also for associated companies, e.g. parent/subsidiary companies, or company ownership.
  Do not use this for employee/employer relationships or any other information.

  - relationship_schema should only be Family for familial relationships between people,
    otherwise use UnknownLink.
  - related_entity_name is the name or names of one related entity.
    Use distinct RelatedEntity entries for each related entity.
  - relationship - use the exact string used to describe the relationship in the text,
    otherwise leave as `null` - e.g. "daughter" or "subsidiary".
"""


def parse_address(node: Element):
    address = {
        "remarks": node.findtext("./remarks"),
        "co": node.findtext("./c-o"),
        "location": node.findtext("./location"),
        "address-details": node.findtext("./address-details"),
        "p-o-box": node.findtext("./p-o-box"),
        "zip-code": node.findtext("./zip-code"),
        "area": node.findtext("./area"),
        "country": node.findtext("./country"),
    }
    return {k: v for (k, v) in address.items() if v is not None}


def compose_address(
    context: Context, entity: Entity, place, el: Element, country_prop: str = "country"
) -> Optional[Entity]:
    addr = dict(place)
    addr.update(parse_address(el))
    entity.add(country_prop, addr.get("country"))
    po_box = addr.get("p-o-box")
    if po_box is not None:
        po_box = f"P.O. Box {po_box}"
    return h.make_address(
        context,
        remarks=addr.get("remarks"),
        summary=addr.get("co"),
        street=addr.get("address-details"),
        city=addr.get("location"),
        po_box=po_box,
        postal_code=addr.get("zip-code"),
        region=addr.get("area"),
        country=addr.get("country"),
    )


def parse_name(context: Context, entity: Entity, node: Element):
    # verification:
    # al-Nu'Aymi   - in full name
    # Lutsky   - Lutsky Ihar Uladzimiravich as primary name

    name_prop = NAME_TYPE[node.get("name-type")]
    is_weak = NAME_QUALITY_WEAK[node.get("quality")]
    if is_weak:
        name_prop = "weakAlias"

    max_order: int = 0
    parts: List[Tuple[str, MayStr, MayStr, int, str]] = []
    for part_node in node.findall("./name-part"):
        part_type = part_node.get("name-part-type")
        order_str = part_node.get("order")
        assert order_str is not None and part_type is not None
        order = int(order_str)
        max_order = max(order, max_order)
        value = part_node.findtext("./value")
        if value is None:
            continue
        parts.append((part_type, None, None, order, value))

        for spelling in part_node.findall("./spelling-variant"):
            lang = registry.language.clean(spelling.get("lang"))
            script = spelling.get("script")
            if spelling.text is None:
                continue
            parts.append((part_type, lang, script, order, spelling.text))

    ordered: Dict[Tuple[MayStr, MayStr], Dict[int, List[MayStr]]] = {}
    for part_type, lang, script, order, value in parts:
        # if part_type in ("suffix", "title"):
        #     print("XXX", part_type, value)

        # Begin building whole names:
        cult = (lang, script)
        if cult not in ordered:
            ordered[cult] = {}
        if order not in ordered[cult]:
            ordered[cult][order] = []
        if part_type == "title":
            ordered[cult][order].append(None)
        else:
            ordered[cult][order].append(value)

        if part_type not in NAME_PARTS:
            context.log.warn("Unknown name part", part_type=part_type)
            continue
        part_prop = NAME_PARTS[part_type]
        if part_type == "whole-name":
            part_prop = name_prop
        if part_prop == "name" and lang is not None:
            part_prop = "alias"
        if part_prop is not None:
            entity.add(part_prop, value, lang=lang, quiet=True)

    for (lang, script), ords in ordered.items():
        whole_parts: List[List[MayStr]] = []
        for order in range(1, max_order + 1):
            values = ords.get(order, ordered[(None, None)][order])
            whole_parts.append(values)

        for prod in product(*whole_parts):
            whole_name = join_text(*prod)
            full_prop = name_prop
            if full_prop == "name" and lang is not None:
                full_prop = "alias"
            entity.add(full_prop, whole_name, lang=lang)


def parse_identity(context: Context, entity: Entity, node: Element, places):
    for name in node.findall(".//name"):
        parse_name(context, entity, name)

    for address_node in node.findall(".//address"):
        place = places.get(address_node.get("place-id"))
        address = compose_address(context, entity, place, address_node)
        h.apply_address(context, entity, address)

    for bday in node.findall(".//day-month-year"):
        bval = parse_parts(bday.get("year"), bday.get("month"), bday.get("day"))
        if entity.schema.is_a("Person"):
            entity.add("birthDate", bval)
        else:
            entity.add("incorporationDate", bval)

    for nationality in node.findall(".//nationality"):
        country = nationality.find("./country")
        if country is not None:
            entity.add("nationality", country.get("iso-code"))
            entity.add("nationality", country.text)

    for bplace in node.findall(".//place-of-birth"):
        place = places.get(bplace.get("place-id"))
        address = compose_address(
            context, entity, place, bplace, country_prop="birthCountry"
        )
        if address is not None:
            entity.add("birthPlace", address.get("full"))

    for doc in node.findall(".//identification-document"):
        type_ = doc.get("document-type")
        is_passport = type_ in ("passport", "diplomatic-passport")
        doc_country = doc.findtext("./issuer")
        entity.add("nationality", doc_country, quiet=True)
        passport = h.make_identification(
            context,
            entity,
            number=doc.findtext("./number"),
            doc_type=type_,
            country=doc_country,
            summary=doc.findtext("./remark"),
            start_date=doc.findtext("./date-of-issue"),
            end_date=doc.findtext("./expiry-date"),
            key=doc.get("ssid"),
            passport=is_passport,
        )
        if passport is not None:
            context.emit(passport)


def make_related_entities(
    context: Context, entity: Entity, relationship: RelatedEntity
) -> List[Entity]:
    other = context.make("LegalEntity")
    other.id = context.make_id(relationship.related_entity_name)
    other.add("name", relationship.related_entity_name)

    res = context.lookup("relations", relationship.relationship_schema)

    rel = context.make(relationship.relationship_schema)
    rel.id = context.make_id(entity.id, relationship.relationship, other.id)
    rel.add(res.text, relationship.relationship)
    rel.add(res.source, entity.id)
    rel.add(res.target, other.id)

    return [rel, other]


def parse_entry(context: Context, target: Element, programs, places):
    entity = context.make("LegalEntity")
    entity_ssid = target.get("ssid")
    if entity_ssid in SKIP_OLD:
        context.log.info("Skipping old entry", ssid=entity_ssid)
        return
    node = target.find("./entity")
    if node is None:
        node = target.find("./individual")
        entity = context.make("Person")
    if node is None:
        node = target.find("./object")
        if node is None:
            context.log.error("No target", target=target)
            return
        object_type = node.get("object-type")
        if object_type != "vessel":
            context.log.warning(
                "Unknown target type", target=target, object_type=object_type
            )
        entity = context.make("Vessel")

    entity.id = context.make_slug(entity_ssid)
    entity.add("gender", node.get("sex"), quiet=True)

    ssid = target.get("sanctions-set-id")
    if ssid is None:
        ssid = target.findtext("./sanctions-set-id")

    sanction = h.make_sanction(
        context,
        entity,
        program_name=programs.get(ssid),
        source_program_key=programs.get(ssid),
        program_key=h.lookup_sanction_program_key(context, programs.get(ssid)),
    )
    sanction.add("authorityId", entity_ssid)
    last_modification = None
    last_modification_type = None
    dates = set()
    for mod in target.findall("./modification"):
        mod_type = mod.get("modification-type")
        effective_date = mod.get("effective-date")
        if effective_date is not None:
            if last_modification is None or effective_date > last_modification:
                last_modification = effective_date
                last_modification_type = mod_type
        dates.add(mod.get("publication-date"))
        if mod_type == "de-listed":
            sanction.add("endDate", effective_date)
            continue
        sanction.add("listingDate", mod.get("publication-date"))
        sanction.add("startDate", effective_date)
    dates_ = [d for d in dates if d is not None]
    if len(dates_):
        entity.add("createdAt", min(dates_))
        entity.add("modifiedAt", max(dates_))

    if last_modification_type == "de-listed":
        return
    entity.add("topics", "sanction")

    foreign_id = target.findtext("./foreign-identifier")
    sanction.add("unscId", foreign_id)

    justifications: List[Tuple[str, str]] = []
    for justification in node.findall("./justification"):
        ssid = justification.get("ssid")
        justifications.append((ssid, justification.text))

    # TODO: should this go into sanction:reason?
    notes = [n for (s, n) in sorted(justifications)]
    entity.add("notes", h.clean_note("\n\n".join(notes)))

    related_entities = []
    for other in node.findall("./other-information"):
        if other.text is None:
            continue
        value = other.text.strip()
        if not value:
            continue
        source_value = TextSourceValue(
            key_parts=value, label="other-information", text=value
        )

        # Import regex-based parsing to reviews if possible
        item = None

        imo_num = REGEX_IMO.fullmatch(value)
        reg_num = REGEX_REGNUM.fullmatch(value)
        inn_match = REGEX_INN.fullmatch(value)
        if imo_num:
            item = SimpleValue(property="imoNumber", value=imo_num.group(1))
        elif entity.schema.is_a("LegalEntity") and value.startswith(
            "Date of registration"
        ):
            _, reg_date = value.split(":", 1)
            item = SimpleValue(property="incorporationDate", value=reg_date.strip())
        elif entity.schema.is_a("LegalEntity") and value.startswith("Type of entity"):
            _, legalform = value.split(":", 1)
            item = SimpleValue(property="legalForm", value=legalform)
        elif entity.schema.is_a("LegalEntity") and reg_num:
            item = SimpleValue(property="registrationNumber", value=reg_num.group(3))
        elif inn_match:
            item = SimpleValue(property="innCode", value=inn_match.group(1))
        elif tax := REGEX_TAX.fullmatch(value):
            item = SimpleValue(property="taxNumber", value=tax.group(1))
        elif website := REGEX_WEBSITE.fullmatch(value):
            item = SimpleValue(property="website", value=website.group(1))
        elif email := REGEX_EMAIL.fullmatch(value):
            item = SimpleValue(property="email", value=email.group(2))
        elif phonenumber := REGEX_PHONE.fullmatch(value):
            item = SimpleValue(property="phone", value=phonenumber.group(3))
        elif value == "Registration number: ИНН":
            pass

        if item is not None:
            extraction = OtherInfo(simple_values=[item])
            review = review_extraction(
                context=context,
                crawler_version=CRAWLER_VERSION,
                source_value=source_value,
                original_extraction=extraction,
                default_accepted=True,
                origin="regex",
            )
        else:
            prompt = PROMPT.format(schema=entity.schema.name)
            extraction = run_typed_text_prompt(
                context, prompt, value, OtherInfo, model=LLM_VERSION
            )
            review = review_extraction(
                context=context,
                crawler_version=CRAWLER_VERSION,
                source_value=source_value,
                original_extraction=extraction,
                origin=LLM_VERSION,
            )
        if not review.accepted:
            entity.add("notes", h.clean_note(value))
            continue

        for extracted_value in review.extracted_data.simple_values:
            prop = entity.schema.get(extracted_value.property)
            if prop is not None:
                if prop.type == registry.date:
                    h.apply_date(
                        entity, extracted_value.property, extracted_value.value
                    )
                    continue
                entity.add(
                    extracted_value.property,
                    extracted_value.value,
                    original_value=value,
                    origin=review.origin,
                )
            else:
                if extracted_value.property == "imoNumber":
                    # If the target wasn't explicitly a vessel, skip the IMO because
                    # it might be of a vessel related to the company, and not the target's IMO.
                    # <justification ssid="10301">IRISL front company...
                    #     It is the registered owner of a vessel owned by IRISL or an IRISL affiliate.
                    # </justification>
                    # <relation ssid="21013" target-id="9702" relation-type="related-to"></relation>
                    # <other-information ssid="10302">No C 38181; IMO number of the vessel: 9387803.</other-information>
                    entity.add("notes", h.clean_note(value))
                    continue
                context.log.warning(
                    "Unknown property for schema",
                    property=extracted_value.property,
                    schema=entity.schema.name,
                    string=value,
                    id=entity.id,
                )
        for relationship in review.extracted_data.related_entities:
            related_entities.extend(
                make_related_entities(context, entity, relationship)
            )
        if review.extracted_data.include_in_notes:
            entity.add("notes", h.clean_note(value))

    for relation in node.findall("./relation"):
        rel_type = relation.get("relation-type")
        target_id = context.make_slug(relation.get("target-id"))
        res = context.lookup("relations", rel_type)
        if res is None:
            context.log.warn(
                "Unknown relationship type",
                type=rel_type,
                source=entity,
                target=target_id,
            )
            continue

        rel = context.make(res.schema)
        rel.id = context.make_slug(relation.get("ssid"), rel_type)
        rel.add(res.source, entity.id)
        rel.add(res.target, target_id)
        rel.add(res.text, rel_type)

        # rel_target = context.make(rel.schema.get(res.target).range)
        # rel_target.id = target_id
        # context.emit(rel_target)
        source_prop = rel.schema.get(res.source)
        if source_prop is not None and source_prop.range is not None:
            entity.add_schema(source_prop.range)
        context.emit(rel)

    for identity in node.findall("./identity"):
        parse_identity(context, entity, identity, places)

    context.emit(entity)
    context.emit(sanction)
    for related_entity in related_entities:
        context.emit(related_entity)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    root = doc.getroot()
    assert root is not None
    # date = root.get("date")
    # if date is not None:
    #     context.data_time = datetime.strptime(date, "%Y-%m-%d")

    # TODO(Leon Handreke): Add a lookup to see if a new sanctions program shows up that we don't have in the database
    programs: Dict[str, MayStr] = {}
    for sanc in doc.findall(".//sanctions-program"):
        sanc_set = sanc.find("./sanctions-set")
        if sanc_set is None:
            context.log.warning("No sanctions-set", program=sanc)
            continue
        ssid = sanc_set.get("ssid")
        if ssid is None:
            continue
        programs[ssid] = sanc.findtext('./program-name[@lang="eng"]')

    places = {}
    for place in doc.findall(".//place"):
        places[place.get("ssid")] = parse_address(place)

    for target in doc.findall("./target"):
        parse_entry(context, target, programs, places)

    assert_all_accepted(context, raise_on_unaccepted=False)
