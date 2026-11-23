from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, row: dict[str, HtmlElement]) -> None:
    # Link to the case summary.
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

    # Unknown columns are left for audit_data.
    for column in list(cells):
        res = context.lookup("action_columns", column)
        if res is None:
            continue
        # Each action column holds an expiry date, "For life" (no end date) or "N/A"
        # (action not imposed). The type.date lookup drops both non-dates, so the
        # sanction has no endDate and the memo term below can fill the dates.
        value = cells.pop(column)
        if value == "N/A":
            continue
        sanction = h.make_sanction(
            context, person, key=res.value, program_key="US-HHS-ORI"
        )
        h.apply_date(sanction, "endDate", value)
        # Fall back to the memo's term when the column gives no date.
        term = memo_terms.get(res.value)
        if term is not None and not sanction.has("endDate"):
            h.apply_date(sanction, "startDate", term["start"])
            h.apply_date(sanction, "endDate", term["end"])
        sanction.add("provisions", res.value)
        sanction.add("summary", memo)
        sanction.add("sourceUrl", link)
        if h.is_active(sanction):
            person.add("topics", res.topic)
        context.emit(sanction)

    context.emit(person)
    # Requests to journals, not restrictions on the researcher.
    context.audit_data(
        cells,
        ignore=["request_retraction_of_article_s", "request_correction_of_article_s"],
    )


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    table = h.xpath_element(doc, "//table[@class='report_table']")
    for row in h.parse_html_table(table):
        crawl_row(context, row)
