from typing import Dict

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


def flat(multiline):
    return multiline.replace("\n", " ")


def crawl_item(row: Dict[str, str], context: Context):
    address = h.make_address(
        context,
        street=flat(row.pop("address_1")),
        street2=flat(row.pop("address_2")),
        postal_code=row.pop("zip").replace("\n", ""),
        city=flat(row.pop("city")),
        state=flat(row.pop("state")),
        country_code="us",
    )
    npi = row.pop("npi")
    first_name = row.pop("first_name")
    last_name = row.pop("last_name")
    # Header is sometimes on the second row
    # Setting 'headers_per_page=False' takes the header only from the first page
    # For each page we skip the header row with this check
    # Please remove this check if the header is always on the first row
    if "last name" in last_name.lower():
        return

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
    h.apply_date(sanction, "startDate", row.pop("action_date").replace("\n", ""))
    sanction.add("reason", flat(row.pop("reason_for_exclusion_termination")))
    sanction.add("provisions", flat(row.pop("excluded_terminated")))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_pdf_url(context: Context):
    doc = context.fetch_text(context.data_url)
    # The link is inside a JSON in a script
    info_txt = doc[doc.find("FileLeafRef") :]
    return (
        "https://www.wvmmis.com/WV%20Medicaid%20Provider%20SanctionedExclusion/"
        + info_txt[15 : info_txt.find(",") - 1]
    )


def page_settings(page):
    return page, {"text_x_tolerance": 1}


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.pdf", crawl_pdf_url(context))
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for item in h.parse_pdf_table(
        context,
        path,
        headers_per_page=False,
        page_settings=page_settings,
    ):
        crawl_item(item, context)
