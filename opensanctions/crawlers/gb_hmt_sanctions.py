from pprint import pprint  # noqa
from datetime import datetime
from normality import stringify, collapse_spaces
from followthemoney import model

from opensanctions.util import jointext

NSMAP = {"GB": "http://schemas.datacontract.org/2004/07/"}


def parse_date(date):
    date = stringify(date)
    if date is None:
        return
    date = date.replace("00/00/", "")
    date = date.strip()
    if len(date) == 4:
        return date
    try:
        date = datetime.strptime(date, "%d/%m/%Y")
        return date.date().isoformat()
    except Exception:
        pass
    try:
        date = datetime.strptime(date, "00/%m/%Y")
        return date.date().isoformat()[:7]
    except Exception:
        pass


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
    entity = context.make("Thing")
    entity.make_slug(row.pop("GroupID"))
    sanction = context.make("Sanction")
    sanction.make_id(entity.id, "Sanction")
    sanction.add("entity", entity)
    sanction.add("authority", "HM Treasury Financial sanctions targets")
    sanction.add("country", "gb")

    if row.pop("GroupTypeDescription") == "Individual":
        entity.schema = model.get("Person")
    org_type = row.pop("OrgType", None)
    if org_type in (
        "Enterprise",
        "Company",
        "Public",
        "Banking",
        "Manufacturer",
        "Airline Company",
        "Shipping company",
    ):
        entity.schema = model.get("Company")
    elif org_type in (
        "Government",
        "Special Police Unit",
        "Government, Ministry",
        "Government Ministry",
        "Department within Government/Military Unit.",
        "Department within Government",
        "Military Government",
        "Military",
    ):
        entity.schema = model.get("PublicBody")
    elif org_type in (
        "State Owned Enterprise",
        "University",
        "Port operator",
        "Foundation",
    ):
        entity.schema = model.get("Organization")

    entity.add_cast("LegalEntity", "legalForm", org_type)

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
    dob_day = int(stringify(row.pop("DayOfBirth", "0")))
    dob_month = int(stringify(row.pop("MonthOfBirth", "0")))
    dob_year = int(stringify(row.pop("YearOfBirth", "0")))
    if dob_year > 1000:
        try:
            dt = datetime(dob_year, dob_month, dob_day)
            entity.add_cast("Person", "birthDate", dt.date())
        except ValueError:
            entity.add_cast("Person", "birthDate", dob_year)

    entity.add_cast("Person", "gender", row.pop("Gender", None))
    id_number = row.pop("NationalIdNumber", None)
    entity.add_cast("LegalEntity", "idNumber", id_number)
    passport = row.pop("PassportDetails", None)
    entity.add_cast("Person", "passportNumber", passport)

    reg_number = row.pop("BusinessRegNumber", None)
    entity.add_cast("LegalEntity", "registrationNumber", reg_number)

    phones = split_items(row.pop("PhoneNumber", None), comma=True)
    entity.add_cast("LegalEntity", "phone", phones)

    website = split_items(row.pop("Website", None), comma=True)
    entity.add_cast("LegalEntity", "website", website)

    emails = split_items(row.pop("EmailAddress", None), comma=True)
    entity.add_cast("LegalEntity", "email", emails)

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
    name1 = row.pop("name1", None)
    entity.add("firstName", name1, quiet=True)
    name2 = row.pop("name2", None)
    name3 = row.pop("name3", None)
    name4 = row.pop("name4", None)
    name5 = row.pop("name5", None)
    name6 = row.pop("Name6", None)
    entity.add("lastName", name6, quiet=True)
    full_name = row.pop("FullName")
    row.pop("AliasTypeName")
    if row.pop("AliasType") == "AKA":
        entity.add("alias", full_name)
    else:
        entity.add("name", full_name)

    entity.add("nationality", row.pop("Nationality", None), quiet=True)
    entity.add("position", row.pop("Position", None), quiet=True)
    entity.add("country", row.pop("Country", None))
    entity.add("address", row.pop("FullAddress", None))
    entity.add("birthPlace", row.pop("TownOfBirth", None), quiet=True)
    entity.add("country", row.pop("CountryOfBirth", None), quiet=True)

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
        _, field = cell.tag.split("}")
        assert field not in row, field
        row[field] = value
    return row


def crawl(context):
    context.fetch_artifact("source.xml", context.dataset.data.url)
    doc = context.parse_artifact_xml("source.xml")
    root = doc.getroot()

    for el in doc.findall(".//ConsolidatedList", root.nsmap):
        parse_row(context, make_row(el))
