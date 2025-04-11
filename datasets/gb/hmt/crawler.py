from typing import Optional, Dict, Any, List
from banal import first

from rigour.mime.types import XML
from followthemoney.util import join_text
import re

from zavod import Context
from zavod import helpers as h

NUMBER_SPLITS = [f"({x})" for x in range(1, 50)]
PUNCTUATION_SPLITS = [". ", ", "]
REGEX_POSTCODE = re.compile(r"\d+")
# Intentionally single digit except zero to try and reduce false positives
# that might occur in phone numbers, e.g. +44(0)82123456
# or based on real example "(Fax): +375 (17) 123-45-67  (Tel): +375 (17) 234-56-78"
REGEX_PHONE_SPLIT = re.compile(r"\([1-9]\) |\. |, ")


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

REG_NO_TYPES = {
    "INN": ("LegalEntity", "innCode"),
    "KPP": ("Company", "kppCode"),
    "OGRN": ("LegalEntity", "ogrnCode"),
    "Russia INN": ("LegalEntity", "innCode"),
    "Russia KPP": ("Company", "kppCode"),
    "Russia OGRN": ("LegalEntity", "ogrnCode"),
    "Russia TIN": ("LegalEntity", "taxNumber"),
    "TIN": ("LegalEntity", "taxNumber"),
    "Tax ID No.": ("LegalEntity", "taxNumber"),
    "UK Company no.": ("LegalEntity", "registrationNumber"),
}
REG_NO_TYPES_PATTERN = "|".join(re.escape(k) for k in REG_NO_TYPES.keys())
REGEX_REG_NO_TYPES = re.compile(
    rf"^(?P<type>{REG_NO_TYPES_PATTERN})\s*[: –-]\s*(?P<value>\d+)$"
)


def parse_countries(text: Any) -> List[str]:
    countries: List[str] = []
    for country in h.multi_split(text, NUMBER_SPLITS + [". "]):
        country_ = h.remove_bracketed(country)
        if country_ is not None:
            countries.append(country)
    return countries


def parse_companies(context: Context, value: Optional[str]):
    if value is None:
        return []
    result = context.lookup("companies", value)
    if result is None:
        context.log.warning("Company name not mapped", value=value)
        return []
    if result.value == "SAME":
        return [value]
    return result.values


def split_reg_no(text: str):
    text = text.replace("Tax Identification Number: INN", "; INN")
    text = text.replace("INN:", "; INN:")
    text = text.replace("TIN:", "; TIN:")
    text = text.replace("OGRN:", "; OGRN:")
    text = text.replace("KPP:", "; KPP:")
    text = text.replace("Tax ID No", "; Tax ID No")
    text = text.replace("Tax ID:", "; Tax ID:")
    text = text.replace("Tax ID", "; Tax ID")
    text = text.replace("Government Gazette Number", "; Government Gazette Number")
    text = text.replace("Legal Entity Number", "; Legal Entity Number")
    text = text.replace(
        "Business Identification Number", "; Business Identification Number"
    )
    text = text.replace("Tax Identification Number", "; Tax Identification Number")
    return h.multi_split(text, NUMBER_SPLITS + [";", " / "])


def id_reg_no(value: str):
    match = REGEX_REG_NO_TYPES.match(value)
    if match is None:
        return "LegalEntity", "registrationNumber", value
    schema, prop = REG_NO_TYPES[match.group("type")]
    return schema, prop, match.group("value")


def split_phone(text: str):
    # Splits on (1) ... (2) ... if there's more than one index to avoid splitting
    # when it's just an optional part of the number in parens.
    values = REGEX_PHONE_SPLIT.split(text)
    if len(values) > 2:
        return values
    return [text]


def parse_row(context: Context, row: Dict[str, Any]):
    group_type = row.pop("GroupTypeDescription")
    listing_date = row.pop("DateListed")
    designated_date = row.pop("DateDesignated", None)
    last_updated = row.pop("LastUpdated")
    regime_name = row.pop("RegimeName")

    schema = TYPES.get(group_type)
    if schema is None:
        context.log.error("Unknown group type", group_type=group_type)
        return
    entity = context.make(schema)
    entity.id = context.make_slug(row.pop("GroupID"))
    sanction = h.make_sanction(
        context,
        entity,
        program_key=context.lookup_value("sanction.programs", regime_name, None),
        start_date=designated_date,
    )
    sanction.add("authority", row.pop("ListingType", None))
    h.apply_date(sanction, "listingDate", listing_date)
    h.apply_date(entity, "createdAt", listing_date)
    if not entity.has("createdAt"):
        h.apply_date(entity, "createdAt", designated_date)

    sanction.add("authorityId", row.pop("UKSanctionsListRef", None))
    sanction.add("unscId", row.pop("UNRef", None))
    sanction.add("status", row.pop("GroupStatus", None))
    sanction.add("reason", row.pop("UKStatementOfReasons", None))
    sanction.add("summary", row.pop("GroupSanctions", None))
    sanction.add("modifiedAt", row.pop("DateAdditionalSanctions", None))

    h.apply_date(sanction, "modifiedAt", last_updated)
    h.apply_date(entity, "modifiedAt", last_updated)

    # TODO: derive topics and schema from this??
    entity_type = row.pop("Entity_Type", None)
    entity.add_cast("LegalEntity", "legalForm", entity_type)

    orig_reg_number = row.pop("Entity_BusinessRegNumber", "")
    for reg_no in split_reg_no(orig_reg_number):
        schema, prop, value = id_reg_no(reg_no)
        entity.add_cast(schema, prop, value, original_value=orig_reg_number)
    row.pop("Ship_Length", None)
    entity.add_cast("Vessel", "flag", row.pop("Ship_Flag", None))
    flags = h.multi_split(row.pop("Ship_PreviousFlags", None), PUNCTUATION_SPLITS)
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

    text = row.pop("Title", None)
    title = h.multi_split(text, NUMBER_SPLITS)
    entity.add("title", title, quiet=True)

    pobs = h.multi_split(row.pop("Individual_TownOfBirth", None), NUMBER_SPLITS)
    entity.add_cast("Person", "birthPlace", pobs)

    dob = row.pop("Individual_DateOfBirth", None)
    if dob is not None:
        entity.add_schema("Person")
        h.apply_date(entity, "birthDate", dob)

    cob = parse_countries(row.pop("Individual_CountryOfBirth", None))
    entity.add_cast("Person", "birthCountry", cob)

    nationalities = parse_countries(row.pop("Individual_Nationality", None))
    entity.add_cast("Person", "nationality", nationalities)

    positions = h.multi_split(row.pop("Individual_Position", None), NUMBER_SPLITS)
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

    post_code, po_box = h.postcode_pobox(row.pop("PostCode", None))
    if post_code is not None and not REGEX_POSTCODE.search(post_code):
        city = post_code
        post_code = None
    else:
        city = None
    full_address = join_text(
        po_box,
        row.pop("Address1", None),
        city,
        row.pop("Address2", None),
        row.pop("Address3", None),
        row.pop("Address4", None),
        row.pop("Address5", None),
        row.pop("Address6", None),
        post_code,
        sep=", ",
    )

    country_name = first(countries)
    if country_name == "UK":  # Ukraine is a whole thing, people.
        country_name = "United Kingdom"

    address = h.make_address(
        context,
        full=full_address,
        postal_code=post_code,
        po_box=po_box,
        city=city,
        country=country_name,
    )
    h.apply_address(context, entity, address)

    passport_number = row.pop("Individual_PassportNumber", None)
    passport_numbers = h.multi_split(passport_number, NUMBER_SPLITS)
    entity.add_cast("Person", "passportNumber", passport_numbers)
    row.pop("Individual_PassportDetails", None)
    # passport_details = split_items(passport_detail)
    # TODO: where do I stuff this?

    ni_number = row.pop("Individual_NINumber", None)
    ni_numbers = h.multi_split(ni_number, NUMBER_SPLITS)
    entity.add_cast("Person", "idNumber", ni_numbers)
    row.pop("Individual_NIDetails", None)
    # ni_details = split_items(ni_detail)
    # TODO: where do I stuff this?

    phones = row.pop("PhoneNumber", "")
    for phone in REGEX_PHONE_SPLIT.split(phones):
        entity.add_cast("LegalEntity", "phone", phone, original_value=phones)
    emails = row.pop("EmailAddress", None)
    for email in h.multi_split(emails, PUNCTUATION_SPLITS + NUMBER_SPLITS):
        entity.add_cast("LegalEntity", "email", email, original_value=emails)
    websites = row.pop("Website", None)
    for website in h.multi_split(websites, PUNCTUATION_SPLITS):
        entity.add_cast("LegalEntity", "website", website, original_value=websites)

    parent_names = row.pop("Entity_ParentCompany", None)
    for name in parse_companies(context, parent_names):
        parent = context.make("Organization")
        parent.id = context.make_slug("named", name)
        parent.add("name", name)
        context.emit(parent)

        ownership = context.make("Ownership")
        ownership.id = context.make_id(parent.id, "owns", entity.id)
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
    if grp_status == "A":
        sanction.add("provisions", "Asset freeze")
    elif grp_status == "I":
        sanction.add("provisions", "Investment ban")
    else:
        context.log.warning("Unknown GrpStatus", value=grp_status)

    notes = row.pop("OtherInformation", None)
    entity.add("notes", h.clean_note(notes))
    if isinstance(notes, str):
        cryptos = h.extract_cryptos(notes)
        for key, curr in cryptos.items():
            wallet = context.make("CryptoWallet")
            wallet.id = context.make_slug(curr, key)
            wallet.add("currency", curr)
            wallet.add("publicKey", key)
            wallet.add("topics", "sanction")
            wallet.add("holder", entity.id)
            context.emit(wallet)

    context.audit_data(row, ignore=["NonLatinScriptLanguage", "NonLatinScriptType"])

    entity.add("topics", "sanction")
    context.emit(entity)
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
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    el = h.remove_namespace(doc)
    for row_el in el.findall(".//FinancialSanctionsTarget"):
        parse_row(context, make_row(row_el))
