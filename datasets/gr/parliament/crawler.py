from urllib.parse import urljoin
from typing import Tuple, Optional

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


# XPath to select all MPs from the dropdown
MP_XPATH = ".//select[@id='ctl00_ContentPlaceHolder1_dmps_mpsListId']/option"
# List of IDs to skip:
# 1. These MPs are not in the dataset
# 2. Only a few of them have missing tables
SKIP_LIST = [
    "3f25e40d-6ca8-4d3c-890c-7d7f51ec4358",
    "2c05592f-07a9-47b9-bed9-b034010b8225",
    "21b5a1ef-26d2-4c09-b11e-b034010b6653",
    "456338e2-4989-4758-af4a-b034010b8e31",
    "7443c8ee-18bb-42b0-9e41-b034010af009",
    "736dc97f-816b-4d52-be1e-b034010aed03",
    "4f3bf776-7361-4450-84a4-b034010b002a",
    "82ba5fbf-0bcc-454e-bd9f-b034010ae40e",
    "09cb7f35-8570-4e7f-8d83-b034010b4f73",
    "4af9ce6a-263a-4f0f-a28c-b034010ad219",
    "ea35c8e4-cd07-4360-be51-b034010b12d2",
    "1294b6b9-9a50-4961-b429-b034010b019d",
    "f8856bfc-54b4-4052-bfe0-b034010b56b3",
    "c9c199ec-4bcd-4bd3-a545-b034010b0907",
    "75f888b4-9245-479c-9aaa-b034010b4284",
    "79bf7490-332e-40c7-a226-b034010b36eb",
    "591f30ee-64a4-4f03-bfd0-b034010b18f2",
]


def lookup_dates(context: Context, term: str) -> Tuple[Optional[str], Optional[str]]:
    dates_lookup_result = context.lookup("term", term)
    if dates_lookup_result is not None:
        dates = dates_lookup_result.dates[0]
        start_date = dates.get("start_date")
        end_date = dates.get("end_date")
        return start_date, end_date
    else:
        context.log.warning("Term not found in lookup", term=term)
        return None, None


def crawl_row(context: Context, str_row, id: str, name, url):
    term = str_row.pop("term")
    start_date, end_date = lookup_dates(context, term)

    person = context.make("Person")
    person.id = context.make_slug(id)
    person.add("name", name)
    person.add("description", str_row.pop("description"))
    person.add("political", str_row.pop("party"))
    person.add("sourceUrl", url)

    position = h.make_position(
        context,
        name="Member of the Hellenic Parliament",
        country="gr",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
        wikidata_id="Q18915989",
    )
    categorisation = categorise(context, position, True)

    if categorisation.is_pep:
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start_date,
            end_date=end_date,
            categorisation=categorisation,
            # Some end dates for previous terms are not available
            no_end_implies_current=False,
        )
        if occupancy:
            context.emit(position)
            context.emit(occupancy)
            context.emit(person)

    context.audit_data(str_row, ["date", "costituency"])


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    # Extract MP IDs and names
    ids = doc.xpath(f"{MP_XPATH}/@value")
    names = doc.xpath(f"{MP_XPATH}/text()")
    # Skip the first option ('Select Member')
    mp_ids_names: list[tuple[str, str]] = list(zip(ids[1:], names[1:]))
    for id, name in mp_ids_names:
        url = urljoin(context.data_url, f"?MpId={id}")
        mp_page = context.fetch_html(url, cache_days=1)
        table = mp_page.xpath(".//table[@class='grid']")
        # Warn if table is missing for MPs not in skip list
        if not table and id not in SKIP_LIST:
            context.log.warning(
                "No table found for MP not in skip list", id=id, url=url
            )
        # Skip processing if table is missing
        if not table:
            continue
        assert len(table) == 1, len(table)
        table = table[0]
        for row in h.parse_html_table(table, ignore_colspan={"5"}):
            str_row = h.cells_to_str(row)
            crawl_row(context, str_row, id, name, url)
