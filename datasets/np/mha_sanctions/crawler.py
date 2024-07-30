import csv
from typing import Dict
import zavod.helpers as h
from zavod import Context

DATE_FORMATS = ["%d-%b-%Y", "%m/%d/%Y", "%d/%m/%Y", "%Y"]


def parse_dates(date_str: str):
    """Parse multiple dates from a string."""
    dates = date_str.split(";")  # Split on ';' if there are multiple dates
    parsed_dates = []
    for date in dates:
        date = date.strip()
        parsed_date = h.parse_date(date, DATE_FORMATS)
        if parsed_date:
            parsed_dates.append(parsed_date)
        else:
            print(f"Warning: Could not parse date: {date}")
    return parsed_dates


def crawl_row(context: Context, row: Dict[str, str]):
    unsc_id = row.pop("code")
    entity_type = row.pop("type")
    name = row.pop("name")
    # Split `alias` on `;` and trim any extra whitespace
    alias = row.pop("alias").split(";")
    birth_dates = parse_dates(row.pop("date of birth"))
    birth_place = row.pop("place of birth").split(";")
    nationality = row.pop("nationality").split(";")
    pass_no = row.pop("passport no").split(";")
    national_id = row.pop("national id")
    address = row.pop("address").split(";")
    listing_date = h.parse_date(row.pop("listed on"), DATE_FORMATS)
    sanction_status = row.pop("sanction status")
    remarks = row.pop("remarks")

    entity = None
    if entity_type == "INDIVIDUALS":
        entity = context.make("Person")
        entity.id = context.make_id(name, birth_dates, unsc_id)
        entity.add("name", name)
        for a in alias:  # Add aliases
            entity.add("alias", a.strip())
        for d_list in birth_dates:
            for d in d_list:
                entity.add("birthDate", d)
        for p in birth_place:
            entity.add("birthPlace", p.strip())
        for n in nationality:
            entity.add("nationality", n.strip())
            # Handle passport numbers
        for p in pass_no:
            p_parts = p.lower().split(" number: ")
            if len(p_parts) == 2:
                country = p_parts[0].strip()
                number = p_parts[1].strip()
                # Create an identification object associated with the entity
                passport = h.make_identification(
                    context, entity=entity, country=country, number=number, passport=True
                )
                if passport is not None:
                    context.emit(passport)
            else:
                entity.add("passportNumber", p.strip())
        entity.add("idNumber", national_id)
        for a in address:
            entity.add("address", address)
        entity.add("notes", remarks)
        sanction = h.make_sanction(context, entity)
        sanction.add("listingDate", listing_date)
        sanction.add("unscId", unsc_id)
        context.emit(sanction)
        if sanction_status == "ACTIVE":
            entity.add("topics", "sanction")
            context.emit(entity, target=True)
        elif sanction_status == "REMOVED":
            context.emit(entity, target=False)

    elif entity_type == "GROUP":
        entity = context.make("Organization")
        entity.id = context.make_id(name, address, unsc_id)
        entity.add("name", name)
        entity.add("notes", remarks)
        for a in alias:  # Add aliases
            entity.add("alias", a.strip())
        for a in address:
            entity.add("address", a.strip())
        sanction = h.make_sanction(context, entity)
        sanction.add("listingDate", listing_date)
        sanction.add("unscId", unsc_id)
        context.emit(sanction)
        if sanction_status == "ACTIVE":
            entity.add("topics", "sanction")
            context.emit(entity, target=True)
        elif sanction_status == "REMOVED":
            context.emit(entity, target=False)
    else:
        context.log.warning("Unhandled entity type", type=entity_type)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
