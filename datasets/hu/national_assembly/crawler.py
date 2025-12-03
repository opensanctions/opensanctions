from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, url: str, raw_name: str, party: str) -> None:
    unblock_pep = ".//div[@class='pair-content']"
    pep_doc = zyte_api.fetch_html(context, url, unblock_pep, cache_days=5)

    entity = context.make("Person")
    entity.id = context.make_id(raw_name, party)

    for honorific in ["Dr.", "Prof."]:
        if raw_name.startswith(honorific):
            raw_name = raw_name[len(honorific) :].strip()

    last_name, first_name = raw_name.split(",", 1)

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
    unblock = ".//table[@class=' table table-bordered']"
    doc = zyte_api.fetch_html(context, context.data_url, unblock, cache_days=5)
    table = doc.find(unblock)
    assert table is not None

    for row in h.parse_html_table(table, skiprows=1, index_empty_headers=True):
        name_el = row["members_of_parliament"]
        url = h.xpath_strings(name_el, ".//a/@href", expect_exactly=1)[0]
        raw_name = h.element_text(name_el)
        party = h.element_text(row["parliamentary_groups"])

        crawl_row(context, url, raw_name, party)
