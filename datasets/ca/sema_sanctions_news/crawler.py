from itertools import chain
from lxml.etree import _Element
from typing import Dict

from nomenklatura.resolver import Linker

from zavod import Context, Entity, helpers as h
from zavod.integration import get_dataset_linker
from zavod.stateful.review import assert_all_accepted

PROGRAM_KEY = "CA-SEMA"
MAX_AGE_DAYS = 15


def crawl_entity_notice(context: Context, row: Dict[str, _Element]) -> None:
    str_row = h.cells_to_str(row)
    listing_date = str_row.pop("date")
    assert listing_date is not None

    # skip sanctions whose listing date is older than MAX_AGE_DAYS
    if not h.within_max_age(context, listing_date, MAX_AGE_DAYS):
        return

    country = str_row.pop("country")
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

    for raw_name in chain(persons, oranizations):
        entity = context.make(schema)
        entity.id = context.make_id(raw_name, country)
        assert entity.id is not None

        born_patterns = [" (born ", " (bornon "]
        for born in born_patterns:
            if born in raw_name.lower():
                name, dob = raw_name.split(born, 1)
                dob = dob.strip(")")
                h.apply_date(entity, "birthDate", dob)
                break
            else:
                name = raw_name

        h.apply_reviewed_name_string(context, entity, string=name)

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


def crawl_vessel(
    context: Context, row: Dict[str, _Element], linker: Linker[Entity]
) -> None:
    str_row = h.cells_to_str(row)
    imo_number = str_row.pop("ship_imo_number")
    name = str_row.pop("ship_name")
    vessel = context.make("Vessel")
    vessel.id = context.make_id(name, imo_number)
    assert vessel.id is not None

    # time-based check does not apply to vessels because
    # the news table doesn't provide their individual ListingDates
    # drop entities already present in [ca_dfatd_sema_sanctions]
    canonical_id = linker.get_canonical(vessel.id)
    already_present = False
    for other_id in linker.get_referents(canonical_id):
        if other_id.startswith("ca-sema") and not other_id.startswith("ca-sema-news"):
            already_present = True
            break
    if already_present:
        return

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
    linker = get_dataset_linker(context.dataset)
    for row in h.parse_html_table(entities_table):
        crawl_entity_notice(context, row)

    ship_table = h.xpath_element(doc, '//table[contains(@class, "table wb-tables")]')
    for row in h.parse_html_table(ship_table):
        crawl_vessel(context, row, linker)

    assert_all_accepted(context, raise_on_unaccepted=False)
