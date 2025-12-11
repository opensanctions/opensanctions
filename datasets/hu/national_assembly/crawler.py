from zavod.extract import zyte_api
from zavod.entity import Entity
from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h


def categorise_and_emit(
    context: Context,
    entity: Entity,
    position: Entity,
    start_date: str | None,
    end_date: str | None,
    is_pep: bool | None,
) -> None:
    categorisation = categorise(context, position, is_pep=is_pep)
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


def crawl_row(context: Context, url: str, raw_name: str, party: str) -> None:
    validator = ".//div[@class='pair-content']"
    pep_doc = zyte_api.fetch_html(context, url, validator, cache_days=5)
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
    # Table with all parliamentary terms
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
        categorise_and_emit(
            context,
            entity=entity,
            position=position,
            start_date=str_row.pop("from"),
            end_date=str_row.pop("to", None),
            is_pep=True,
        )
    # Table with state functions, e.g. 'Parliamentary Secretary for Finance'
    state_functions_table = h.xpath_elements(
        pep_doc, '//table[.//th[@colspan="3" and text()="State functions"]]'
    )
    if len(state_functions_table) > 0:
        for row in h.parse_html_table(
            state_functions_table[0],
            skiprows=1,
            ignore_colspan={"3"},
            index_empty_headers=True,
        ):
            str_row = h.cells_to_str(row)
            function = str_row.pop("function")
            if not function:
                continue
            position = h.make_position(
                context,
                name=function,
                country="hu",
                topics=["gov.national"],
            )
            categorise_and_emit(
                context,
                entity=entity,
                position=position,
                start_date=str_row.pop("from"),
                end_date=str_row.pop("to", None),
                # is_pep is not set, since not all these positions are necessarily PEP positions
                is_pep=None,
            )


def crawl(context: Context) -> None:
    table_xpath = ".//table[@class=' table table-bordered']"
    doc = zyte_api.fetch_html(context, context.data_url, table_xpath, cache_days=5)
    table = doc.find(table_xpath)
    assert table is not None

    for row in h.parse_html_table(table, skiprows=1, index_empty_headers=True):
        name_el = row["members_of_parliament"]
        url = h.xpath_strings(name_el, ".//a/@href", expect_exactly=1)[0]
        raw_name = h.element_text(name_el)
        party = h.element_text(row["parliamentary_groups"])

        crawl_row(context, url, raw_name, party)
