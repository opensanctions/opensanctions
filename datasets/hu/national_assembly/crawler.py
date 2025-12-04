from lxml import html
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h


def split_party_name(context: Context, full_name: str) -> tuple[str, str | None]:
    """Split MP name and party affiliation from format 'Surname, Given (PARTY)'."""
    if "(" not in full_name:
        context.log.warning(f"No party found in: {full_name}")
        return full_name, None
    name, party = full_name.rsplit("(", 1)
    return name.strip(), party.rstrip(")").strip()


def parse_name(context: Context, full_name: str) -> tuple[str, str]:
    """Strip title prefix and return (last_name, first_name) from 'Last, First' format."""
    name = full_name.strip().removeprefix("Dr. ").removeprefix("Prof. ")
    if "," not in name:
        context.log.warning(f"Expected comma-separated name: {full_name}")
        return "", name
    last, first = (part.strip() for part in name.split(",", 1))
    return last, first


def crawl_row(context: Context, row: html.Element) -> None:
    url_el = row.find(".//a[@href]")
    if url_el is None:
        context.log.warning("No URL found in row, skipping")
        return
    url = url_el.get("href")
    assert url is not None, "No URL found in row"
    validator = ".//div[@class='pair-content']"
    pep_doc = zyte_api.fetch_html(context, url, validator, cache_days=5)

    name_party_raw = h.xpath_elements(
        pep_doc, ".//div[@class='pair-content']//h1", expect_exactly=1
    )[0]
    name_party = h.element_text(name_party_raw)
    name, party = split_party_name(context, name_party)
    last_name, first_name = parse_name(context, name)

    entity = context.make("Person")
    entity.id = context.make_id(name, party)
    h.apply_name(entity, first_name=first_name, last_name=last_name)
    entity.add("political", party)
    entity.add("sourceUrl", url)
    entity.add("citizenship", "hu")

    position = h.make_position(
        context,
        name="Member of the National Assembly of Hungary",
        wikidata_id="Q17590876",
        country="hu",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )

    elections_table = h.xpath_elements(
        pep_doc, '//table[.//th[@colspan="5" and text()="Elections"]]', expect_exactly=1
    )[0]

    for row in h.parse_html_table(
        elections_table,
        skiprows=1,
        ignore_colspan={"5"},
        index_empty_headers=True,
    ):
        str_row = h.cells_to_str(row)
        start_date = str_row.pop("from")
        end_date = str_row.pop("to", None)

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            return

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            start_date=start_date,
            end_date=end_date,
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(entity)


def crawl(context: Context) -> None:
    table_xpath = ".//table[@class=' table table-bordered']"
    doc = zyte_api.fetch_html(context, context.data_url, unblock, cache_days=5)
    table = doc.find(unblock)
    assert table is not None, table
    rows = table.findall(".//tbody/tr")
    for row in rows:
        crawl_row(context, row)
