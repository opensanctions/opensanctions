from typing import Dict
from normality import collapse_spaces
from prefixdate import parse

from opensanctions.core import Context
from opensanctions import helpers as h


def parse_date(date):
    return h.parse_date(date, ["%d-%m-%Y"])


def crawl_row(context: Context, data: Dict[str, str]):
    entity = context.make("LegalEntity")
    entity.id = context.make_slug(data.pop("INDIVIDUAL_Id"))
    assert entity.id, data
    entity.add("notes", h.clean_note(data.pop("COMMENTS", None)))
    entity.add("notes", h.clean_note(data.pop("NOTE", None)))
    entity.add("notes", h.clean_note(data.pop("NOTE1", None)))
    entity.add("notes", h.clean_note(data.pop("NOTE2", None)))
    entity.add("notes", h.clean_note(data.pop("NOTE3", None)))
    entity.add_cast("Person", "nationality", data.pop("NATIONALITY", None))
    entity.add_cast("Person", "title", data.pop("TITLE", None))
    entity.add_cast("Person", "position", data.pop("DESIGNATION", None))
    entity.add_cast("Person", "birthPlace", data.pop("PLACEOFBIRTH", None))
    entity.add_cast("Person", "birthPlace", data.pop("CITY_OF_BIRTH", None))
    entity.add_cast("Person", "birthDate", data.pop("YEAR", None))
    entity.add_cast("Person", "gender", h.clean_gender(data.pop("GENDER", None)))
    entity.add_cast("Person", "birthDate", parse_date(data.pop("DATE", None)))
    entity.add_cast("Person", "birthDate", parse_date(data.pop("DATE_OF_BIRTH", None)))

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

    entity.add_cast("Person", "passportNumber", data.pop("PASSPORT", None))
    data.pop("DATE_OF_ISSUE", None)
    data.pop("CITY_OF_ISSUE", None)
    entity.add("country", data.pop("COUNTRY_OF_ISSUE", None))
    entity.add_cast("Person", "idNumber", data.pop("IDNUMBER", None))

    address = h.make_address(
        context,
        # remarks=data.pop("NOTE"),
        street=data.pop("STREET", None),
        city=data.pop("CITY", None),
        region=data.pop("STATE_PROVINCE", None),
        postal_code=data.pop("ZIP_CODE", None),
        country=data.pop("COUNTRY", None),
    )
    h.apply_address(context, entity, address)

    sanction = h.make_sanction(context, entity)
    entity.add("createdAt", parse_date(data.pop("DateInserted", None)))
    sanction.add("startDate", parse_date(data.pop("ListedON", None)))
    sanction.add("startDate", data.pop("FROM_YEAR", None))
    sanction.add("endDate", data.pop("TO_YEAR", None))
    sanction.add("program", data.pop("UN_LIST_TYPE", None))
    sanction.add("reason", data.pop("SUBMITTED_BY", None))
    sanction.add("unscId", data.pop("REFERENCE_NUMBER", None))

    entity.add("topics", "sanction")
    h.audit_data(data, ignore=["VERSIONNUM", "TYPE_OF_DATE"])
    context.emit(entity, target=True)


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
