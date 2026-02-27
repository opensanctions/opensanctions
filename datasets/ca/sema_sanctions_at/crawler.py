from itertools import chain
from lxml.etree import _Element
from typing import Dict

from zavod import Context, helpers as h


PROGRAM_KEY = "CA-SEMA"


def crawl_entity(context: Context, row: Dict[str, _Element]) -> None:
    str_row = h.cells_to_str(row)
    country = str_row.pop("country")
    listing_date = str_row.pop("date")
    schema_type = str_row.pop("types")
    res = context.lookup("schema_type", schema_type, warn_unmatched=True)
    schema = res.value if res else None
    if not schema:
        return

    specific_prohibition = row.pop("specific_prohibition")
    reason = h.xpath_strings(specific_prohibition, ".//p[3]//text()")
    people = h.xpath_strings(
        specific_prohibition, ".//div[contains(., 'List of individuals')]//li/text()"
    )
    entities = h.xpath_strings(
        specific_prohibition, ".//div[contains(., 'List of entities')]//li/text()"
    )
    for name in chain(people, entities):
        entity = context.make(schema)
        entity.id = context.make_id(name)

        # Clean the name and extract DOB if present
        if "(born " in name.lower():
            name, dirty_dob = name.split("(born", 1)
            # entity.add("name", name.strip())
            h.apply_date(entity, "birthDate", dirty_dob)

        # Clean up aliases with Russian names
        if "(russian:" in name.lower():
            name, name_ru = name.split("(Russian:", 1)
            # entity.add("name", name.strip(), lang="eng")
            entity.add("name", name_ru.strip().rstrip(")"), lang="rus")

        # send the rest of the irregular names into review
        original = h.Names(name=name)
        is_irregular, suggested = h.check_names_regularity(entity, original)

        h.review_names(
            context,
            entity,
            original=original,
            suggested=suggested,
            is_irregular=is_irregular,
        )
        # use h.apply_reviewed_names() once the reviews are done?

        entity.add("name", name, lang="eng")
        entity.add("topics", "sanction")

        sanction = h.make_sanction(
            context,
            entity,
            program_name=country,
            source_program_key=country,
            program_key=PROGRAM_KEY,
        )
        sanction.add("program", country)
        sanction.add("reason", reason)
        h.apply_date(sanction, "listingDate", listing_date)

        context.emit(entity)
        context.emit(sanction)


def crawl_vessel(context: Context, row: Dict[str, str | None]) -> None:
    imo_number = row.pop("ship_imo_number")
    name = row.pop("ship_name")
    vessel = context.make("Vessel")
    vessel.id = context.make_id(name, imo_number)
    vessel.add("name", name)
    vessel.add("imoNumber", imo_number)
    vessel.add("type", row.pop("ship_type"))
    h.apply_date(vessel, "buildDate", row.pop("ship_build_date"))
    vessel.add("topics", "sanction")
    sanction = h.make_sanction(
        context,
        vessel,
        program_key=PROGRAM_KEY,
    )
    context.emit(vessel)
    context.emit(sanction)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    entities_table = h.xpath_element(doc, '//table[contains(@id, "dataset-filter1")]')
    for row in h.parse_html_table(entities_table):
        crawl_entity(context, row)

    ship_table = h.xpath_element(doc, '//table[contains(@class, "table wb-tables")]')
    for row in h.parse_html_table(ship_table):
        str_row = h.cells_to_str(row)
        crawl_vessel(context, str_row)
