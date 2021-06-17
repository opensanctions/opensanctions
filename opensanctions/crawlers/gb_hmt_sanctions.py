from pprint import pprint
from normality import stringify, collapse_spaces
from followthemoney import model

from opensanctions.util import remove_namespace
from opensanctions.util import jointext, multi_split, remove_bracketed
from opensanctions.util import date_parts, date_formats, MONTH, YEAR

FORMATS = ["%d/%m/%Y", ("00/%m/%Y", MONTH), ("00/00/%Y", YEAR), ("%Y", YEAR)]
NSMAP = {"GB": "http://schemas.datacontract.org/2004/07/"}
COUNTRY_SPLIT = ["(1)", "(2)", "(3)"]


def parse_date(date):
    return date_formats(date, FORMATS)


def parse_countries(text):
    countries = set()
    for country in multi_split(text, COUNTRY_SPLIT):
        country = remove_bracketed(country)
        countries.add(country)
    return countries


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


def parse_row(context, row):
    group_type = row.pop("GroupTypeDescription")
    org_type = row.pop("OrgType", None)
    if group_type == "Individual":
        base_schema = "Person"
    elif row.get("TypeOfVessel") is not None:
        base_schema = "Vessel"
    elif group_type == "Entity":
        base_schema = context.lookup_value("org_type", org_type, "Organization")
    else:
        context.log.error("Unknown entity type", group_type=group_type)
        return
    entity = context.make(base_schema)
    entity.make_slug(row.pop("GroupID"))
    if org_type is not None:
        entity.add_cast("LegalEntity", "legalForm", org_type)

    sanction = context.make("Sanction")
    sanction.make_id(entity.id, "Sanction")
    sanction.add("entity", entity)
    sanction.add("authority", "HM Treasury Financial sanctions targets")
    sanction.add("country", "gb")

    # entity.add("position", row.pop("Position"), quiet=True)
    entity.add("notes", row.pop("OtherInformation", None), quiet=True)
    entity.add("notes", row.pop("FurtherIdentifiyingInformation", None), quiet=True)

    sanction.add("program", row.pop("RegimeName"))
    sanction.add("authority", row.pop("ListingType", None))
    sanction.add("startDate", parse_date(row.pop("DateListed")))
    sanction.add("recordId", row.pop("FCOId", None))
    sanction.add("status", row.pop("GroupStatus", None))
    sanction.add("reason", row.pop("UKStatementOfReasons", None))

    last_updated = parse_date(row.pop("LastUpdated"))
    if last_updated is not None:
        sanction.add("modifiedAt", last_updated)
        sanction.context["updated_at"] = last_updated
        entity.add("modifiedAt", last_updated)
        entity.context["updated_at"] = last_updated

    # DoB is sometimes a year only
    row.pop("DateOfBirth", None)
    dob = date_parts(
        row.pop("YearOfBirth", 0),
        row.pop("MonthOfBirth", 0),
        row.pop("DayOfBirth", 0),
    )
    if dob is not None:
        entity.add_cast("Person", "birthDate", dob)

    entity.add_cast("Person", "gender", row.pop("Gender", None))
    id_number = row.pop("NationalIdNumber", None)
    entity.add_cast("LegalEntity", "idNumber", id_number)
    passport = row.pop("PassportDetails", None)
    entity.add_cast("Person", "passportNumber", passport)

    flag = row.pop("FlagOfVessel", None)
    entity.add_cast("Vessel", "flag", flag)

    prev_flag = row.pop("PreviousFlags", None)
    entity.add_cast("Vessel", "pastFlags", prev_flag)

    year = row.pop("YearBuilt", None)
    entity.add_cast("Vehicle", "buildDate", year)

    type_ = row.pop("TypeOfVessel", None)
    entity.add_cast("Vehicle", "type", type_)

    imo = row.pop("IMONumber", None)
    entity.add_cast("Vessel", "imoNumber", imo)

    tonnage = row.pop("TonnageOfVessel", None)
    entity.add_cast("Vessel", "tonnage", tonnage)
    row.pop("LengthOfVessel", None)

    # entity.add("legalForm", org_type)
    entity.add("title", row.pop("NameTitle", None), quiet=True)
    entity.add("firstName", row.pop("name1", None), quiet=True)
    entity.add("secondName", row.pop("name2", None), quiet=True)
    entity.add("middleName", row.pop("name3", None), quiet=True)
    entity.add("middleName", row.pop("name4", None), quiet=True)
    entity.add("middleName", row.pop("name5", None), quiet=True)
    name6 = row.pop("Name6", None)
    entity.add("lastName", name6, quiet=True)
    full_name = row.pop("FullName", name6)
    row.pop("AliasTypeName")
    if row.pop("AliasType") == "AKA":
        entity.add("alias", full_name)
    else:
        entity.add("name", full_name)

    countries = parse_countries(row.pop("Nationality", None))
    entity.add("nationality", countries, quiet=True)
    entity.add("position", row.pop("Position", None), quiet=True)

    countries = parse_countries(row.pop("Country", None))
    entity.add("country", countries)
    entity.add("address", row.pop("FullAddress", None))
    entity.add("birthPlace", row.pop("TownOfBirth", None), quiet=True)

    countries = parse_countries(row.pop("CountryOfBirth", None))
    entity.add("country", countries, quiet=True)

    address = jointext(
        row.pop("address1", None),
        row.pop("address2", None),
        row.pop("address3", None),
        row.pop("address4", None),
        row.pop("address5", None),
        row.pop("address6", None),
        row.pop("PostCode", None),
    )
    entity.add("address", address, quiet=True)

    reg_number = row.pop("BusinessRegNumber", None)
    entity.add_cast("LegalEntity", "registrationNumber", reg_number)

    phones = split_items(row.pop("PhoneNumber", None), comma=True)
    entity.add_cast("LegalEntity", "phone", phones)

    website = split_items(row.pop("Website", None), comma=True)
    entity.add_cast("LegalEntity", "website", website)

    emails = split_items(row.pop("EmailAddress", None), comma=True)
    entity.add_cast("LegalEntity", "email", emails)

    # TODO: graph
    row.pop("Subsidiaries", None)
    row.pop("ParentCompany", None)
    row.pop("CurrentOwners", None)

    row.pop("DateListedDay", None)
    row.pop("DateListedMonth", None)
    row.pop("DateListedYear", None)
    row.pop("LastUpdatedDay", None)
    row.pop("LastUpdatedMonth", None)
    row.pop("LastUpdatedYear", None)
    row.pop("GrpStatus", None)
    row.pop("ID", None)
    row.pop("DateOfBirthId", None)
    row.pop("DateListedDay", None)
    if len(row):
        pprint(row)

    context.emit(entity, target=True, unique=True)
    context.emit(sanction)


def make_row(el):
    row = {}
    for cell in el.getchildren():
        if cell.text is None:
            continue
        value = cell.text.strip()
        if not len(value):
            continue
        assert cell.tag not in row, cell.tag
        row[cell.tag] = value
    return row


def crawl(context):
    context.fetch_artifact("source.xml", context.dataset.data.url)
    doc = context.parse_artifact_xml("source.xml")
    doc = remove_namespace(doc)

    for el in doc.findall(".//ConsolidatedList"):
        parse_row(context, make_row(el))
