from typing import Any
import orjson
from lxml import html
from lxml.etree import _Element as Element
from rigour.mime.types import JSON

from zavod import Context, helpers as h


def extract_html_field(tree: Element, field_text: str) -> str:
    """
    Extracts the text from the <span> immediately following a <strong>
    containing the given field_text (case-sensitive).
    """
    xpath = (
        f"//strong[contains(text(), '{field_text}')]/following-sibling::span[1]/text()"
    )
    result = h.xpath_strings(tree, xpath)
    return result[0].strip() if result else ""


def crawl_item(context: Context, item: dict[str, Any]) -> None:
    trading_name = item.pop("tradingname")
    sponsor_name = item.pop("sponsorname")
    abn = item.pop("abn")
    obligation_breached = item.pop("obligationbreached")
    # The 'description' field contains an HTML snippet with additional sanction details.
    description = item.pop("description")
    # The HTML contains two additional fields, "Infringement notice issued" and "Amount",
    # which are intentionally ignored as there are no matching properties.
    tree = html.fromstring(description)
    # Extract details from the HTML.
    sanction_imposed = extract_html_field(tree, "Sanction imposed")
    obligation_breached = extract_html_field(tree, "Obligation breached")
    date_of_infringement_notice = extract_html_field(
        tree, "Date of infringement notice"
    )
    # We expect at least one of these fields to be present.
    if not any((sanction_imposed, obligation_breached, date_of_infringement_notice)):
        context.log.warn(
            "No expected fields found in the 'description' HTML. Structure may have changed."
        )

    entity = context.make("Company")
    entity.id = context.make_id(trading_name, abn)
    entity.add("name", trading_name)
    entity.add("name", sponsor_name)
    entity.add("registrationNumber", abn)
    entity.add("country", "au")
    address = h.make_address(
        context, state=item.pop("state"), postal_code=item.pop("postcode")
    )
    h.copy_address(entity, address)

    sanction = h.make_sanction(
        context,
        entity,
        start_date=item.pop("sanctionimposeddatetext"),
        end_date=item.pop("sanctionenddate"),
    )
    sanction.add("reason", obligation_breached)
    sanction.add("provisions", sanction_imposed)
    sanction.add("provisions", item.pop("sanctionimposed"))
    h.apply_date(sanction, "date", date_of_infringement_notice)
    h.apply_date(sanction, "date", item.pop("sanctioninfrinoticedate"))

    if h.is_active(sanction):
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        item,
        ignore=[
            "sanctionimposeddate",  # is the same as sanctionimposeddatetext
            "amountstatus",
            "sanctioninfrinoticeamount",
            "sanctioninfribreachnotice",  # is usually covered by obligationbreached
        ],
    )


def crawl(context: Context) -> None:
    path = context.fetch_resource(
        "source.json",
        context.data_url,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json;odata=verbose",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
        },
        method="POST",
    )
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "rb") as fh:
        data = orjson.loads(fh.read())
    for item in data.get("d", {}).get("data", []):
        crawl_item(context, item)
