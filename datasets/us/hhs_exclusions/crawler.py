import csv
from typing import Dict, Any

from zavod import helpers as h
from zavod import Context

FORMATS = ["%Y%m%d"]


def is_zero(value: str) -> bool:
    return all(c == "0" for c in value)


def crawl_item(context: Context, row: Dict[str, Any]):
    city = row.pop("CITY")
    zip_code = row.pop("ZIP")
    first_name = row.pop("FIRSTNAME")
    last_name = row.pop("LASTNAME")
    middle_name = row.pop("MIDNAME")
    bus_name = row.pop("BUSNAME")
    id_name = h.make_name(
        full=bus_name,
        first_name=first_name,
        middle_name=middle_name,
        last_name=last_name,
    )
    entity = context.make("LegalEntity")
    entity.id = context.make_slug(id_name, zip_code, city, strict=False)
    if entity.id is None:
        context.log.warning(
            "No id for entity",
            name=id_name,
            zip_code=zip_code,
            city=city,
        )
        return

    if first_name or last_name:
        entity.add_schema("Person")
        h.apply_name(
            entity=entity,
            first_name=first_name,
            middle_name=middle_name,
            last_name=last_name,
            lang="eng",
        )
        entity.add("birthDate", h.parse_date(row.pop("DOB", None), FORMATS))
        general_role = row.pop("GENERAL")
        specialism = row.pop("SPECIALTY")
        if specialism:
            position = f"{specialism} ({general_role})"
        else:
            position = general_role
        entity.add("position", position)
    else:
        entity.add_schema("Company")
        entity.add("name", bus_name)
        entity.add("description", row.pop("GENERAL") or None)
        entity.add("description", row.pop("SPECIALTY") or None)

    address = h.make_address(
        context,
        street=row.pop("ADDRESS"),
        city=city,
        state=row.pop("STATE"),
        postal_code=zip_code,
        country_code="us",
    )
    h.copy_address(entity, address)
    upin = row.pop("UPIN")
    if upin:
        entity.add("description", f"UPIN: {upin}")
    npi = row.pop("NPI")
    if npi:
        entity.add("description", f"NPI: {npi}")
    entity.add("topics", "debarment")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", h.parse_date(row.pop("EXCLDATE"), FORMATS))
    sanction.add("reason", row.pop("EXCLTYPE"))
    sanction.add("program", "US HHS OIG List of Excluded Individuals/Entities ")
    waiver_start = row.pop("WAIVERDATE")
    waiver_state = row.pop("WVRSTATE")
    if waiver_start:
        waiver_description = (
            f"Waiver start date: {h.parse_date(waiver_start, FORMATS)[0]}"
        )
        if waiver_state:
            waiver_description += ", "
    else:
        waiver_description = ""
    if waiver_state:
        waiver_description += f"State: {waiver_state}"
    if waiver_description:
        waiver_description += ", Read more: https://oig.hhs.gov/exclusions/waivers.asp"
        sanction.add("provisions", waiver_description)
    reinvdate = row.pop("REINDATE")
    assert reinvdate is None, reinvdate
    context.emit(sanction)
    context.emit(entity, target=True)
    context.audit_data(row)


def crawl(context: Context) -> None:
    source_file = context.fetch_resource("source.csv", context.data_url)
    with open(source_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key, value in row.items():
                if is_zero(value):
                    row[key] = None
            crawl_item(context, row)
