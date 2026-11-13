from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, row: dict[str, HtmlElement]) -> None:
    # The name cells all link to the person's case summary page.
    link = h.xpath_strings(row["last_name"], ".//a/@href", expect_exactly=1)[0]
    cells = h.cells_to_str(row)

    person = context.make("Person")
    person.id = context.make_id(link)
    h.apply_name(
        person,
        first_name=cells.pop("first_name"),
        middle_name=cells.pop("middle_name"),
        last_name=cells.pop("last_name"),
    )
    person.add("country", "us")
    person.add("sourceUrl", link)
    memo = cells.pop("memo")
    memo_res = context.lookup("memo_terms", memo, warn_unmatched=True)
    memo_terms = (memo_res.terms if memo_res is not None else None) or {}

    # Columns not in the lookup stay in `cells` and are flagged by audit_data.
    for column in list(cells):
        res = context.lookup("action_columns", column)
        if res is None:
            continue
        # An expiry date, "For life" (dropped by the type.date lookup), or "N/A"
        # when the action was not imposed.
        value = cells.pop(column)
        if value == "N/A":
            continue
        if value is None:
            context.log.warning("Empty action term", person=link, column=column)
            continue
        sanction = h.make_sanction(
            context, person, key=res.value, program_key="US-HHS-ORI"
        )
        term = memo_terms.get(res.value)
        if term is not None:
            h.apply_date(sanction, "startDate", term["start"])
            h.apply_date(sanction, "endDate", term["end"])
        else:
            h.apply_date(sanction, "endDate", value)
        sanction.add("provisions", res.value)
        sanction.add("summary", memo)
        sanction.add("sourceUrl", link)
        if h.is_active(sanction):
            person.add("topics", res.topic)
        context.emit(sanction)

    context.emit(person)
    # Retraction and correction requests are addressed to journals, not restrictions
    # on the researcher, and have no FollowTheMoney equivalent.
    context.audit_data(
        cells,
        ignore=["request_retraction_of_article_s", "request_correction_of_article_s"],
    )


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    table = h.xpath_element(doc, "//table[@class='report_table']")
    for row in h.parse_html_table(table):
        crawl_row(context, row)
