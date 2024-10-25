from typing import Dict, Optional, List
import re

from zavod import Context
from zavod import helpers as h


REGEX_PASSPORT = re.compile(r"^[A-Z0-9-]{6,20}$")


def parse_date(date: Optional[str]) -> List[str]:
    if date is None:
        return []
    dates = set()
    for dp in h.multi_split(date, [", "]):
        dates.update(h.parse_date(dp[:10], ["%d-%m-%Y", "%Y-%m-%d", "%Y-%m"]))
    return list(dates)


def clean_passports(context: Context, text: str) -> List[str]:
    values = text.split(", ")
    passports = []
    ids = []
    is_id = None
    for value in values:
        if not value:
            continue
        if value.lower() == "national identification number":
            is_id = True
        elif value.lower() in "passport":
            is_id = False
        elif REGEX_PASSPORT.search(value):
            if is_id:
                ids.append(value)
            else:
                passports.append(value)
            is_id = None
        else:
            passports.append(value)
            is_id = None
    return passports, ids



# def crawl_person(context: Context, data: Dict[str, str]):


def crawl_row(context: Context, data: Dict[str, str]):
    entity = context.make("LegalEntity")
    full_name = data.pop("FullName", None)
    ind_id = data.pop("INDIVIDUAL_Id", data.pop("IndividualID"))
    entity.id = context.make_slug(ind_id, full_name)
    assert entity.id, data
    entity.add("name", full_name)
    entity.add("notes", h.clean_note(data.pop("Comments", None)))
    entity.add("address", data.pop("IndividualAddress", None))
    entity.add_cast("Person", "nationality", data.pop("Nationality", None))
    entity.add_cast("Person", "title", data.pop("Title", None))
    entity.add_cast("Person", "position", data.pop("Designation", None))
    entity.add_cast("Person", "birthPlace", data.pop("IndividualPlaceOfBirth", None))
    dob = parse_date(data.pop("IndividualDateOfBirth", None))
    entity.add_cast("Person", "birthDate", dob)

    alias = data.pop("IndividualAlias", None)
    if alias is not None: 
        for a in h.multi_split(alias, [", ", "Good", "Low"]):
            entity.add("alias", a)

    passports, ids = clean_passports(context, data.pop("IndividualDocument", ""))
    entity.add_cast("Person", "passportNumber", passports)
    entity.add_cast("Person", "idNumber", ids)

    sanction = h.make_sanction(context, entity)
    listed_on = data.pop("ListedOn", None)
    listed_at = parse_date(listed_on)
    entity.add("createdAt", listed_at)
    sanction.add("listingDate", listed_at)
    sanction.add("unscId", data.pop("ReferenceNumber", None))

    entity.add("topics", "sanction")
    context.audit_data(data, ignore=["ApplicationStatus"])
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource(
        "source.xml",
        context.data_url,
        method="POST",
        data={"fileType": "xml"},
    )
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)

    for row in doc.findall(".//Table"):
        data = {}
        for field in row.getchildren():
            value = field.text
            if value == "NA":
                continue
            data[field.tag] = value
        crawl_row(context, data)
    # for row in doc.findall(".//Table1"):
    #     data = {}
    #     for field in row.getchildren():
    #         value = field.text
    #         if value == "NA":
    #             continue
    #         data[field.tag] = value
    #     crawl_row(context, data)
