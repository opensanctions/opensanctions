from typing import Any
import re

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

LANDING_PAGE_URL = (
    "https://data.public.lu/en/datasets/?q=bourgmestre+échevins+élections"
)


def crawl_record(
    context: Context,
    record: dict[str, Any],
) -> None:
    commune_code = record.pop("COM_CODE")
    commune_label = record.pop("COM_LABEL")
    position_label = record.pop("MAN_LABEL")
    first_name = record.pop("ELU_FIRST_NAME")
    last_name = record.pop("ELU_LAST_NAME")
    gender = record.pop("ELU_SEX")
    start_date = record.pop("COAC_START_DATE")
    end_date = record.pop("COAC_END_DATE")

    res = context.lookup("position", position_label)
    assert res is not None, f"Unknown position: {position_label!r}"
    position_name = res.value

    position = h.make_position(
        context,
        name=f"{position_name} of {commune_label}",
        country="lu",
        subnational_area=commune_label,
        lang="fra",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, commune_code)
    h.apply_name(person, first_name=first_name, last_name=last_name, lang="fra")
    person.add("gender", gender)
    # citizenship not required: https://guichet.public.lu/fr/citoyens/citoyennete/elections/elections-communales/candidat-elections-communales.html
    person.add("country", "lu")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        no_end_implies_current=True,
        start_date=start_date,
        end_date=end_date or None,
        propagate_country=True,
    )
    if occupancy is not None:
        context.emit(position)
        context.emit(occupancy)
        context.emit(person)

    context.audit_data(record)


def crawl(context: Context) -> None:
    landing_page = context.fetch_html(
        LANDING_PAGE_URL, cache_days=1, absolute_links=True
    )
    results = h.xpath_elements(
        landing_page, ".//ul[contains(@class, 'search-results')]/li"
    )
    # one dataset is expected:
    assert len(results) == 1

    # notify in case of new elections
    text = h.xpath_strings(results[0], ".//a[contains(@href, '/datasets/')]/text()")
    match = re.search(r"\b(20\d{2})\b", text[0])
    if match and match.group(1) != "2023":
        context.log.info(
            "Expected dataset for 2023 elections, but found different year: %s",
            match.group(1),
        )
    if match is None:
        context.log.warning("Could not determine election year from dataset title")

    # fetch the data
    data = context.fetch_json(context.data_url, cache_days=1)
    assert isinstance(data, list), f"Expected list, got {type(data)}"

    for record in data:
        crawl_record(context, record)
