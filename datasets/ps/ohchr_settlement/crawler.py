import re

from lxml.etree import _Element as Element
from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "OHCHR-BHR"


def crawl_row(
    context: Context,
    row: dict[str, Element],
) -> None:
    response_cell = row.pop("response_from_business_enterprise")
    str_row = h.cells_to_str(row)
    name = str_row.pop("business_enterprise")
    home_state = str_row.pop("home_state")
    activities = str_row.pop("listed_activity_subparagraph_of_paragraph_96")
    involvement = str_row.pop(
        "type_of_involvement_in_adverse_impact_on_the_right_to_self_determination"
    )
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
    entity.add("name", name)
    entity.add("country", home_state)
    entity.add("topics", "debarment")
    if footnote is not None:
        entity.add("previousName", footnote.previousName)
        entity.add("alias", footnote.alias)
    response_links = h.links_to_dict(response_cell)
    if h.element_text(response_cell) != "":
        entity.add("sourceUrl", response_links.pop("response"))
    assert response_links == {}, response_links

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
    if activities is not None:
        sanction.add(
            "reason", f"Listed activities (subparagraph of paragraph 96): {activities}"
        )
    assert involvement is not None, name
    sanction.add(
        "summary",
        f"Type of involvement in adverse impact on the right to self-determination: {involvement}",
    )

    context.audit_data(str_row, ["no"])
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
    for row in h.parse_html_table(table):
        crawl_row(context, row)
