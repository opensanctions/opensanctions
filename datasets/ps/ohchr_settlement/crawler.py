import re

from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "OHCHR-BHR"


def crawl_row(
    context: Context,
    str_row: dict[str, str | None],
    seen_rows: dict[str, str | None],
) -> None:
    name = str_row.pop("business_enterprise")
    home_state = str_row.pop("home_state")
    assert name is not None and home_state is not None, str_row

    footnote = context.lookup("name_footnotes", name)
    if footnote is not None:
        name = footnote.name
    # Footnote markers are a lowercase letter after the final full stop of the
    # name, directly or after a space: "Davidov Garages Ltd.b".
    elif re.search(r"\. ?[a-z]$", name):
        context.log.warning(
            "Name looks like it ends with a footnote marker. Add it to the "
            "name_footnotes lookup.",
            name=name,
        )

    entity = context.make("Company")
    entity.id = context.make_id(name, home_state)
    assert entity.id is not None, str_row
    # The ID is built from the name and the home state, so two rows that repeat
    # both are stored as one company and the entity count drops below the number
    # of rows in list A without any other trace in the run.
    previous_no = seen_rows.get(entity.id)
    if previous_no is not None:
        context.log.warning(
            "Row repeats the name and home state of an earlier row, so both "
            "rows are emitted as one company.",
            name=name,
            home_state=home_state,
            no=str_row["no"],
            previous_no=previous_no,
        )
    seen_rows[entity.id] = str_row["no"]
    entity.add("name", name)
    entity.add("country", home_state)
    entity.add("topics", "debarment")
    if footnote is not None:
        entity.add("previousName", footnote.previousName)
        entity.add("alias", footnote.alias)

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)

    context.audit_data(
        str_row,
        [
            # The row number is not stable enough to build an ID from: rows
            # are numbered alphabetically, so the numbers shift when a report
            # adds or removes an enterprise.
            "no",
            "response_from_business_enterprise",
            # The listed activities are only letters that refer to the
            # subparagraphs of paragraph 96 in A/HRC/22/63, which makes them hard
            # to crawl into useful text. The type of involvement is not very
            # informative. Both are left out.
            "listed_activity_subparagraph_of_paragraph_96",
            "type_of_involvement_in_adverse_impact_on_the_right_to_self_determination",
        ],
    )
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    content_xpath = "//div[contains(@class, 'ohchr-layout__container')]"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        content_xpath,
        absolute_links=True,
        cache_days=1,
    )
    content = h.xpath_elements(doc, content_xpath, expect_exactly=1)[0]
    # The hash covers the list of reports and both tables. Before you accept a
    # new hash, review the tables and the count in the dataset assertions.
    h.assert_dom_hash(
        content, "4a6e0e7baff37cba3362997e9a2f889f80a307dc", text_only=True
    )

    # List B holds enterprises that are no longer involved in listed
    # activities, so only list A is crawled.
    section = h.xpath_elements(
        content,
        ".//h5[normalize-space(.)='A. Business enterprises involved in listed activities']"
        "/following-sibling::div[1]",
        expect_exactly=1,
    )[0]
    table = h.xpath_elements(section, ".//table", expect_exactly=1)[0]
    seen_rows: dict[str, str | None] = {}
    for row in h.parse_html_table(table):
        crawl_row(context, h.cells_to_str(row), seen_rows)
