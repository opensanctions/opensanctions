from typing import Dict

from opensanctions.core import Context
from opensanctions import helpers as h


def parse_date(date):
    return h.parse_date(date, ["%d-%m-%Y"])


def crawl_row(context: Context, data: Dict[str, str]):
    entity = context.make("LegalEntity")
    ind_id = data.pop("INDIVIDUAL_Id", data.pop("IndividualID"))
    entity.id = context.make_slug(ind_id)
    assert entity.id, data
    entity.add("notes", h.clean_note(data.pop("COMMENTS", None)))
    entity.add("notes", h.clean_note(data.pop("Comments", None)))
    entity.add("notes", h.clean_note(data.pop("NOTE", None)))
    entity.add("notes", h.clean_note(data.pop("NOTE1", None)))
    entity.add("notes", h.clean_note(data.pop("NOTE2", None)))
    entity.add("notes", h.clean_note(data.pop("NOTE3", None)))
    entity.add_cast("Person", "nationality", data.pop("NATIONALITY", None))
    entity.add_cast("Person", "nationality", data.pop("Nationality", None))
    entity.add_cast("Person", "title", data.pop("TITLE", None))
    entity.add_cast("Person", "title", data.pop("Title", None))
    entity.add_cast("Person", "position", data.pop("DESIGNATION", None))
    entity.add_cast("Person", "position", data.pop("Designation", None))
    entity.add_cast("Person", "birthPlace", data.pop("PLACEOFBIRTH", None))
    entity.add_cast("Person", "birthPlace", data.pop("IndividualPlaceOfBirth", None))
    entity.add_cast("Person", "birthPlace", data.pop("CITY_OF_BIRTH", None))
    entity.add_cast("Person", "birthDate", data.pop("YEAR", None))
    entity.add_cast("Person", "gender", data.pop("GENDER", None))
    entity.add_cast("Person", "birthDate", parse_date(data.pop("DATE", None)))
    entity.add_cast("Person", "birthDate", parse_date(data.pop("DATE_OF_BIRTH", None)))
    dob = parse_date(data.pop("IndividualDateOfBirth", None))
    entity.add_cast("Person", "birthDate", dob)

    data.pop("BIRTHPLACE_x0020_CITY", None)
    data.pop("BIRTHPLACE_x0020_STATE_PROVINCE", None)
    entity.add("country", data.pop("BIRTHPLACE_x0020_COUNTRY", None))
    entity.add("country", data.pop("COUNTRY_OF_BIRTH", None))
    entity.add_cast("Person", "birthPlace", data.pop("BIRTHPLACE_x0020_NOTE", None))

    h.apply_name(
        entity,
        full=data.pop("FullName", None),
        given_name=data.pop("FIRST_NAME", None),
        second_name=data.pop("SECOND_NAME", None),
        name3=data.pop("THIRD_NAME", None),
        name4=data.pop("FOURTH_NAME", None),
        quiet=True,
    )

    alias = data.pop("NAME_ORIGINAL_SCRIPT", None)
    if alias is not None and "?" not in alias:
        entity.add("alias", alias)
    entity.add("alias", data.pop("SORT_KEY", None))
    data.pop("IndividualAlias", None)

    entity.add_cast("Person", "passportNumber", data.pop("PASSPORT", None))
    entity.add_cast("Person", "passportNumber", data.pop("IndividualDocument", None))
    data.pop("DATE_OF_ISSUE", None)
    data.pop("CITY_OF_ISSUE", None)
    entity.add("country", data.pop("COUNTRY_OF_ISSUE", None))
    entity.add_cast("Person", "idNumber", data.pop("IDNUMBER", None))

    address = h.make_address(
        context,
        # remarks=data.pop("NOTE"),
        full=data.pop("IndividualAddress", None),
        street=data.pop("STREET", None),
        city=data.pop("CITY", None),
        region=data.pop("STATE_PROVINCE", None),
        postal_code=data.pop("ZIP_CODE", None),
        country=data.pop("COUNTRY", None),
    )
    h.apply_address(context, entity, address)

    sanction = h.make_sanction(context, entity)
    inserted_at = parse_date(data.pop("DateInserted", None))
    listed_on = data.pop("ListedON", data.pop("ListedOn", None))
    listed_at = parse_date(listed_on)
    entity.add("createdAt", inserted_at or listed_at)
    sanction.add("listingDate", listed_at or inserted_at)
    sanction.add("startDate", data.pop("FROM_YEAR", None))
    sanction.add("endDate", data.pop("TO_YEAR", None))
    sanction.add("program", data.pop("UN_LIST_TYPE", None))
    sanction.add("unscId", data.pop("REFERENCE_NUMBER", None))
    sanction.add("unscId", data.pop("ReferenceNumber", None))
    sanction.add("authority", data.pop("SUBMITTED_BY", None))

    entity.add("topics", "sanction")
    h.audit_data(data, ignore=["VERSIONNUM", "TYPE_OF_DATE", "ApplicationStatus"])
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
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
