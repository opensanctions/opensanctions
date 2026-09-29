import re

from lxml.etree import _Element as Element
from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "OHCHR-BHR"


def parse_footnotes(section: Element) -> dict[str, tuple[str, str]]:
    """Map each footnote marker to the kind of footnote and the name it gives."""
    footnotes: dict[str, tuple[str, str]] = {}
    for paragraph in h.xpath_elements(section, ".//p"):
        # Footnotes are separated by <br/>, so each text node is one footnote.
        for line in h.xpath_strings(paragraph, "./text()"):
            line = line.strip()
            if line == "":
                continue
            match = re.match(
                r"^(?P<marker>[a-z])- (?P<kind>Formerly|Previously listed as) (?P<name>.+)$",
                line,
            )
            if match is None:
                raise ValueError(f"Unexpected footnote: {line!r}")
            name = match.group("name")
            # The full stop that ends the footnote sentence merges with a
            # trailing "Ltd.", so it is only removed from other names.
            if not name.endswith(" Ltd."):
                name = name.removesuffix(".")
            marker = match.group("marker")
            assert marker not in footnotes, marker
            footnotes[marker] = (match.group("kind"), name)
    return footnotes


def crawl_row(
    context: Context,
    row: dict[str, Element],
    footnotes: dict[str, tuple[str, str]],
    used_markers: set[str],
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

    footnote: tuple[str, str] | None = None
    # A footnote marker is a lowercase letter appended to the name, directly or
    # after a space: "Davidov Garages Ltd.b", "Ashtrom Residential Development Ltd. a".
    marker_match = re.match(r"^(?P<name>.+\.) ?(?P<marker>[a-z])$", name)
    if marker_match is not None:
        marker = marker_match.group("marker")
        if marker not in footnotes:
            raise ValueError(f"Footnote marker {marker!r} has no footnote: {name!r}")
        assert marker not in used_markers, marker
        used_markers.add(marker)
        footnote = footnotes[marker]
        name = marker_match.group("name")

    entity = context.make("Company")
    entity.id = context.make_id(name, home_state)
    entity.add("name", name)
    entity.add("country", home_state)
    entity.add("topics", "debarment")
    if footnote is not None:
        kind, footnote_name = footnote
        if kind == "Formerly":
            entity.add("previousName", footnote_name)
        else:
            entity.add("alias", footnote_name)
    response_links = h.links_to_dict(response_cell)
    if h.element_text(response_cell) != "":
        entity.add("sourceUrl", response_links.pop("response"))
    assert response_links == {}, response_links

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
    if activities is not None:
        sanction.add(
            "reason", f"Listed activities (subparagraph of paragraph 96): {activities}"
        )
    res = context.lookup("type_of_involvement", involvement)
    if res is None:
        raise ValueError(f"Unknown type of involvement: {involvement!r}")
    sanction.add(
        "summary",
        f"Type of involvement in adverse impact on the right to self-determination: {res.value}",
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
    footnotes = parse_footnotes(section)
    used_markers: set[str] = set()
    for row in h.parse_html_table(table):
        crawl_row(context, row, footnotes, used_markers)
    unused_markers = footnotes.keys() - used_markers
    if unused_markers:
        raise ValueError(f"Footnotes without a row: {unused_markers}")
