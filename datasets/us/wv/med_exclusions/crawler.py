from zavod import Context, helpers as h
from rigour.mime.types import PDF


prompt = """
 Extract structured data from the following page of a PDF document. Return
  a JSON list (`providers`) in which each object represents an medical provider.
  Each object should have the following fields: `last_name`, `first_name`,
  `npi`, `address_1`, `address_2`, `city`, `state`, `zip`, `action_date`,
  `excluded_terminated`, `reason`.
  Return an empty string for unset fields.
"""


def flat(multiline: str | None) -> str:
    return (multiline or "").replace("\n", " ")


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    address = h.make_address(
        context,
        street=flat(row.pop("address_1")),
        street2=flat(row.pop("address_2")),
        postal_code=(row.pop("zip") or "").replace("\n", ""),
        city=flat(row.pop("city")),
        state=flat(row.pop("state")),
        country_code="us",
    )
    npi = row.pop("npi")
    first_name = row.pop("first_name")
    last_name = row.pop("last_name")

    if first_name:
        entity = context.make("Person")
        entity.id = context.make_id(first_name, first_name, npi)
        h.apply_name(entity, first_name=flat(first_name), last_name=flat(last_name))
    else:
        entity = context.make("Company")
        entity.id = context.make_id(last_name, npi)
        entity.add("name", flat(last_name))

    entity.add("npiCode", h.multi_split(npi, [",", "\n"]))
    entity.add("topics", "debarment")
    entity.add("country", "us")
    h.apply_address(context, entity, address)

    sanction = h.make_sanction(context, entity)
    h.apply_date(
        sanction,
        "startDate",
        (row.pop("action_date") or "").replace("\n", ""),
        # Exclusions cannot predate the 1977 Medicare-Medicaid Anti-Fraud and Abuse
        # Amendments.
        two_digit_year_base=1977,
    )
    sanction.add("reason", flat(row.pop("reason_for_exclusion_termination")))
    sanction.add("provisions", flat(row.pop("excluded_terminated")))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_pdf_url(context: Context) -> str:
    doc = context.fetch_html(context.data_url)
    # Construct the URL from bits in the table because they'd rather do that than just construct a working URL.
    rows = h.xpath_elements(doc, "//tr[@class='rgRow']")  # RadGrid row
    assert len(rows) == 1, len(rows)
    row = rows[0]
    link_element = h.xpath_element(
        row, ".//a[contains(@href, 'javascript:__doPostBack')]"
    )
    doc_name = h.element_text(link_element).strip().replace(" ", "%20")
    parent_folder = h.element_text(
        h.xpath_element(row, ".//td[@style='display:none;']")
    ).strip()
    return f"https://www.wvmmis.com/SharepointDownload?parent={parent_folder}&docname={doc_name}"


def crawl(context: Context) -> None:
    path = context.fetch_resource(
        "source.pdf", crawl_pdf_url(context), headers={"Referer": context.data_url}
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for item in h.parse_pdf_table(
        context,
        path,
        # The header row is repeated as the first row of the table on every page.
        headers_per_page=True,
        page_settings=lambda page: (page, {"text_x_tolerance": 1}),
    ):
        crawl_item(item, context)
