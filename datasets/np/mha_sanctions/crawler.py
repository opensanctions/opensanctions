import csv
from typing import Dict

from zavod import Context, helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    unsc_id = row.pop("code")
    entity_type = row.pop("type")
    name = row.pop("name")
    # Split `alias` on `;` and trim any extra whitespace
    alias = row.pop("alias").split(";")
    birth_dates = h.multi_split(row.pop("date of birth"), ";")
    birth_place = row.pop("place of birth").split(";")
    nationality = row.pop("nationality").split(";")
    pass_no = row.pop("passport no").split(";")
    national_id = row.pop("national id")
    address = row.pop("address").split(";")
    listing_date = row.pop("listed on")
    sanction_status = row.pop("sanction status")
    remarks = row.pop("remarks")

    entity = None
    if entity_type == "INDIVIDUALS":
        entity = context.make("Person")
        entity.id = context.make_id(name, national_id, unsc_id)
        entity.add("name", name)
        for a in alias:  # Add aliases
            entity.add("alias", a.strip())
        for date in birth_dates:
            h.apply_date(entity, "birthDate", date)
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
                    context,
                    entity=entity,
                    country=country,
                    number=number,
                    passport=True,
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
        h.apply_date(sanction, "listingDate", listing_date)
        sanction.add("unscId", unsc_id)
        context.emit(sanction)
        if sanction_status == "ACTIVE":
            entity.add("topics", "sanction")
            context.emit(entity)
        else:
            if not sanction_status == "REMOVED":
                context.log.warning("Unexpected sanction status", entity=entity.id)
            context.emit(entity)

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
        h.apply_date(sanction, "listingDate", listing_date)
        sanction.add("unscId", unsc_id)
        context.emit(sanction)
        if sanction_status == "ACTIVE":
            entity.add("topics", "sanction")
            context.emit(entity)
        else:
            if not sanction_status == "REMOVED":
                context.log.warning(
                    "Unexpected sanction status",
                    value=sanction_status,
                    entity=entity.id,
                )
            context.emit(entity)
    else:
        context.log.warning("Unhandled entity type", type=entity_type)


def crawl(context: Context):
    doc = context.fetch_html(context.dataset.url, cache_days=1)
    url = doc.xpath(".//a[contains(text(), 'Sanction_List_for_All.pdf')]/@href")
    assert len(url) == 1, "Expected exactly 1 link in the document"
    h.assert_url_hash(context, url[0], "c31312f5c4680c9ca5e0cdffbe2d9a2d52d29fd5")
    # ALI MAYCHOU
    # ...
    # STATE TRADING COMPANY FOR CONSTRUCTION MATERIALS

    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
