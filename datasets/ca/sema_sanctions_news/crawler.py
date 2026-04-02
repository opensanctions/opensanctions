import re

from itertools import chain
from lxml.etree import _Element
from typing import Dict

from zavod import Context, helpers as h
from zavod.stateful.review import assert_all_accepted

PROGRAM_KEY = "CA-SEMA"


def split_names(name: str) -> tuple[bool, h.Names]:
    """
    Returns:
    - True if any name or alias contains a conjunction (or, and, et), otherwise False
    - categorised and cleaned Names instance
    """
    aliases = []
    is_irregular = False

    if "(also known as" in name.lower():
        name, aka = name.split("(also known as", 1)
        aliases.extend([a.strip().rstrip(")") for a in aka.split(",")])

    if "(également connue sous le nom" in name.lower():
        name, aka = name.split("(également connue sous le nom de ", 1)
        aliases.extend([a.strip().rstrip(")") for a in aka.split(",")])

    # some names contain digits at the beginning of the string
    name = re.sub(r"^\d+\s*", "", name)

    suggested = h.Names(name=name.strip(), alias=aliases)

    for _prop_name, values in suggested.nonempty_item_lists():
        for value in values:
            if re.search(r"\b(or|and|et)\b", value, flags=re.I):
                is_irregular = True
                return is_irregular, suggested

    return is_irregular, suggested


def crawl_entity_notice(context: Context, row: Dict[str, _Element]) -> None:
    str_row = h.cells_to_str(row)
    country = str_row.pop("country")
    listing_date = str_row.pop("date")
    entity_type = str_row.pop("types")
    res = context.lookup("schema_type", entity_type, warn_unmatched=True)
    schema = res.value if res else None
    if not schema:
        return

    specific_prohibition = row.pop("specific_prohibition")
    reason = h.xpath_strings(specific_prohibition, ".//p[3]//text()")
    # clean up non-breaking HTML spaces
    reason = [r.replace("\xa0", " ") for r in reason]

    # At the time of writing, only one of these appear and on the row with appropriate "types" value.
    persons = h.xpath_strings(
        specific_prohibition, ".//div[contains(., 'List of individuals')]//li/text()"
    )
    oranizations = h.xpath_strings(
        specific_prohibition,
        ".//div[contains(., 'List of entities') or contains(., 'Listed entity')]//li/text()",
    )

    # empty list but entity_type is in individuals or entities => skip explicitly
    INSURANCE_PROHIBITION = "prohibited from providing insurance, reinsurance, and underwriting services for Russian aviation and aerospace products"
    if not persons and not oranizations:
        assert INSURANCE_PROHIBITION in reason[0], (
            "Unexpected row with no persons or organizations. "
            f"Country: {country}, listing date: {listing_date}, reason: {reason}",
        )
        return

    assert sum([bool(persons), bool(oranizations)]) == 1, (
        "Expected exactly one of persons or organizations",
        len(persons),
        len(oranizations),
        entity_type,
        listing_date,
    )

    for name in chain(persons, oranizations):
        entity = context.make(schema)
        entity.id = context.make_id(name, country)

        born_patterns = ["(born ", "(bornon "]
        for born in born_patterns:
            if born in name.lower():
                name, dob = name.split(born, 1)
                dob = dob.strip(")")
                h.apply_date(entity, "birthDate", dob)

        # TODO: Add once LangText is merged
        # https://github.com/opensanctions/opensanctions/pull/3770
        if "(russian:" in name.lower():
            name, name_ru = name.split("(Russian:", 1)
            name_ru = name_ru.strip().rstrip(")")
            h.apply_reviewed_name_string(context, entity, string=name_ru, lang="rus")

        original = h.Names(name=name.strip())
        crawler_is_irregular, suggested = split_names(name)
        helper_is_irregular, suggested = h.check_names_regularity(entity, suggested)

        h.apply_reviewed_names(
            context,
            entity,
            original=original,
            suggested=suggested,
            is_irregular=crawler_is_irregular or helper_is_irregular,
        )
        entity.add("topics", "sanction")

        sanction = h.make_sanction(
            context,
            entity,
            program_key=h.lookup_sanction_program_key(context, country),
            program_name=country,
            source_program_key=country,
        )
        sanction.add("reason", reason)
        h.apply_date(sanction, "listingDate", listing_date)

        context.emit(entity)
        context.emit(sanction)


def crawl_vessel(context: Context, row: Dict[str, _Element]) -> None:
    str_row = h.cells_to_str(row)
    imo_number = str_row.pop("ship_imo_number")
    name = str_row.pop("ship_name")
    vessel = context.make("Vessel")
    vessel.id = context.make_id(name, imo_number)
    vessel.add("name", name)
    vessel.add("imoNumber", imo_number)
    vessel.add("type", str_row.pop("ship_type"))
    h.apply_date(vessel, "buildDate", str_row.pop("ship_build_date"))
    vessel.add("topics", "sanction")
    # The program applies to all vessels. Since there is no country column in the
    # vessel table, we cannot map from the original value.
    sanction = h.make_sanction(
        context,
        vessel,
        program_key=PROGRAM_KEY,
    )
    context.emit(vessel)
    context.emit(sanction)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    entities_table = h.xpath_element(doc, '//table[contains(@id, "dataset-filter1")]')
    for row in h.parse_html_table(entities_table):
        crawl_entity_notice(context, row)

    ship_table = h.xpath_element(doc, '//table[contains(@class, "table wb-tables")]')
    for row in h.parse_html_table(ship_table):
        crawl_vessel(context, row)

    assert_all_accepted(context, raise_on_unaccepted=False)
