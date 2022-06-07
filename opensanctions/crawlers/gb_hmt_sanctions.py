from banal import first
from pprint import pprint
from normality import stringify, collapse_spaces
from pantomime.types import XML
from followthemoney.util import join_text

from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import multi_split, remove_bracketed

FORMATS = ["%d/%m/%Y", "00/%m/%Y", "%m/%Y", "00/00/%Y", "%Y"]
COUNTRY_SPLIT = ["(1)", "(2)", "(3)", ". "]

TYPES = {
    "Individual": "Person",
    "Entity": "LegalEntity",
    "Ship": "Vessel",
}

WEAK_QUALITY = {
    "Good quality": False,
    "Low quality": True,
    None: False,
}

NAME_TYPES = {
    "Primary name": "name",
    "Primary name variation": "alias",
    "AKA": "alias",
    "FKA": "previousName",
}


def parse_countries(text):
    countries = set()
    for country in multi_split(text, COUNTRY_SPLIT):
        country = remove_bracketed(country)
        countries.add(country)
    return countries


def parse_companies(context, value):
    if value is None:
        return []
    result = context.lookup("companies", value)
    if result is None:
        context.log.warning("Company name not mapped", value=value)
        return []
    if result.value == "SAME":
        return [value]
    return list(result.values)


def split_items(text, comma=False):
    text = stringify(text)
    if text is None:
        return []
    items = []
    rest = str(text)
    for num in range(50):
        parts = rest.split(f"({num})")
        if len(parts) > 1:
            match = collapse_spaces(parts[0])
            if len(match):
                items.append(match)
            rest = parts[1]
    if comma and text == rest:
        items = text.split(",")
    return items


def split_new(text):
    # It's 2022 and they can't multi-value a thing...
    return multi_split(text, [". ", ", "])


def parse_row(context: Context, row):
    group_type = row.pop("GroupTypeDescription")
    schema = TYPES.get(group_type)
    if schema is None:
        context.log.error("Unknown group type", group_type=group_type)
        return
    entity = context.make(schema)
    entity.id = context.make_slug(row.pop("GroupID"))
    sanction = h.make_sanction(context, entity)
    sanction.add("program", row.pop("RegimeName"))
    sanction.add("authority", row.pop("ListingType", None))
    listed_date = h.parse_date(row.pop("DateListed"), FORMATS)
    sanction.add("listingDate", listed_date)
    designated_date = h.parse_date(row.pop("DateDesignated"), FORMATS)
    sanction.add("startDate", designated_date)

    entity.add("createdAt", listed_date)
    if not entity.has("createdAt"):
        entity.add("createdAt", designated_date)

    sanction.add("authorityId", row.pop("UKSanctionsListRef", None))
    sanction.add("unscId", row.pop("UNRef", None))
    sanction.add("status", row.pop("GroupStatus", None))
    sanction.add("reason", row.pop("UKStatementOfReasons", None))

    last_updated = h.parse_date(row.pop("LastUpdated"), FORMATS)
    sanction.add("modifiedAt", last_updated)
    entity.add("modifiedAt", last_updated)

    # TODO: derive topics and schema from this??
    entity_type = row.pop("Entity_Type", None)
    entity.add_cast("LegalEntity", "legalForm", entity_type)

    reg_number = row.pop("Entity_BusinessRegNumber", None)
    entity.add_cast("LegalEntity", "registrationNumber", reg_number)

    row.pop("Ship_Length", None)
    entity.add_cast("Vessel", "flag", row.pop("Ship_Flag", None))
    flags = split_new(row.pop("Ship_PreviousFlags", None))
    entity.add_cast("Vessel", "pastFlags", flags)
    entity.add_cast("Vessel", "type", row.pop("Ship_Type", None))
    entity.add_cast("Vessel", "tonnage", row.pop("Ship_Tonnage", None))
    entity.add_cast("Vessel", "buildDate", row.pop("Ship_YearBuilt", None))
    entity.add_cast("Vessel", "imoNumber", row.pop("Ship_IMONumber", None))

    ship_owner = row.pop("Ship_CurrentOwners", None)
    if ship_owner is not None:
        owner = context.make("LegalEntity")
        owner.id = context.make_slug("named", ship_owner)
        owner.add("name", ship_owner)
        context.emit(owner)

        ownership = context.make("Ownership")
        ownership.id = context.make_id(entity.id, "owns", owner.id)
        ownership.add("owner", owner)
        ownership.add("asset", entity)
        context.emit(ownership)

    countries = parse_countries(row.pop("Country", None))
    entity.add("country", countries)

    title = split_items(row.pop("Title", None))
    entity.add("title", title, quiet=True)

    pobs = split_items(row.pop("Individual_TownOfBirth", None))
    entity.add_cast("Person", "birthPlace", pobs)

    dob = h.parse_date(row.pop("Individual_DateOfBirth", None), FORMATS)
    entity.add_cast("Person", "birthDate", dob)

    cob = parse_countries(row.pop("Individual_CountryOfBirth", None))
    entity.add_cast("Person", "country", cob)

    nationalities = parse_countries(row.pop("Individual_Nationality", None))
    entity.add_cast("Person", "nationality", nationalities)

    positions = split_items(row.pop("Individual_Position", None))
    entity.add_cast("Person", "position", positions)

    entity.add_cast("Person", "gender", row.pop("Individual_Gender", None))

    name_type = row.pop("AliasType", None)
    name_prop = NAME_TYPES.get(name_type)
    if name_prop is None:
        context.log.warning("Unknown name type", type=name_type)
        return
    name_quality = row.pop("AliasQuality", None)
    is_weak = WEAK_QUALITY.get(name_quality)
    if is_weak is None:
        context.log.warning("Unknown name quality", quality=name_quality)
        return

    h.apply_name(
        entity,
        name1=row.pop("name1", None),
        name2=row.pop("name2", None),
        name3=row.pop("name3", None),
        name4=row.pop("name4", None),
        name5=row.pop("name5", None),
        tail_name=row.pop("Name6", None),
        name_prop=name_prop,
        is_weak=is_weak,
        quiet=True,
    )
    entity.add("alias", row.pop("NameNonLatinScript", None))

    full_address = join_text(
        row.pop("Address1", None),
        row.pop("Address2", None),
        row.pop("Address3", None),
        row.pop("Address4", None),
        row.pop("Address5", None),
        row.pop("Address6", None),
        sep=", ",
    )

    address = h.make_address(
        context,
        full=full_address,
        postal_code=row.pop("PostCode", None),
        country=first(countries),
    )
    h.apply_address(context, entity, address)

    passport_number = row.pop("Individual_PassportNumber", None)
    passport_numbers = split_items(passport_number)
    entity.add_cast("Person", "passportNumber", passport_numbers)
    passport_detail = row.pop("Individual_PassportDetails", None)
    # passport_details = split_items(passport_detail)
    # TODO: where do I stuff this?

    ni_number = row.pop("Individual_NINumber", None)
    ni_numbers = split_items(ni_number)
    entity.add_cast("Person", "idNumber", ni_numbers)
    ni_detail = row.pop("Individual_NIDetails", None)
    # ni_details = split_items(ni_detail)
    # TODO: where do I stuff this?

    for phone in split_new(row.pop("PhoneNumber", None)):
        entity.add_cast("LegalEntity", "phone", phone)

    for email in split_new(row.pop("EmailAddress", None)):
        entity.add_cast("LegalEntity", "email", email)

    for website in split_new(row.pop("Website", None)):
        entity.add_cast("LegalEntity", "website", website)

    for name in parse_companies(context, row.pop("Entity_ParentCompany", None)):
        parent = context.make("Organization")
        parent.id = context.make_slug("named", name)
        parent.add("name", name)
        context.emit(parent)

        ownership = context.make("Ownership")
        ownership.id = context.make_id(entity.id, "owns", parent.id)
        ownership.add("owner", parent)
        ownership.add("asset", entity)
        context.emit(ownership)

    for name in parse_companies(context, row.pop("Entity_Subsidiaries", None)):
        subsidiary = context.make("Company")
        subsidiary.id = context.make_slug("named", name)
        subsidiary.add("name", name)
        context.emit(subsidiary)

        ownership = context.make("Ownership")
        ownership.id = context.make_id(entity.id, "owns", subsidiary.id)
        ownership.add("owner", entity)
        ownership.add("asset", subsidiary)
        context.emit(ownership)

    grp_status = row.pop("GrpStatus", None)
    if grp_status != "A":
        context.log.warning("Unknown GrpStatus", value=grp_status)

    entity.add("notes", h.clean_note(row.pop("OtherInformation", None)))
    h.audit_data(row, ignore=["NonLatinScriptLanguage", "NonLatinScriptType"])

    entity.add("topics", "sanction")
    context.emit(entity, target=True)
    context.emit(sanction)


def make_row(el):
    row = {}
    for cell in el.getchildren():
        nil = cell.get("{http://www.w3.org/2001/XMLSchema-instance}nil")
        if cell.text is None or nil == "true":
            continue
        value = cell.text.strip()
        if not len(value):
            continue
        assert cell.tag not in row, cell.tag
        row[cell.tag] = value
    return row


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = h.remove_namespace(doc)

    for el in doc.findall(".//FinancialSanctionsTarget"):
        parse_row(context, make_row(el))
