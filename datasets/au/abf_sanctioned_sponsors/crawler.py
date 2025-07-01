from lxml import html

from zavod import Context, helpers as h


def access_html(tree, field_text):
    """
    Extracts the text from the <span> immediately following a <strong>
    containing the given field_text (case-sensitive).
    """
    xpath = (
        f"//strong[contains(text(), '{field_text}')]/following-sibling::span[1]/text()"
    )
    result = tree.xpath(xpath)
    return result[0].strip() if result else ""


def crawl_item(context: Context, item: dict):
    trading_name = item.pop("tradingname")
    sponsor_name = item.pop("sponsorname")
    abn = item.pop("abn")
    obligation_breached = item.pop("obligationbreached")
    # The description field contains HTML with more details.
    description = item.pop("description")
    # There are 2 more available fields in the HTML: "Infringement notice issued" and "Amount",
    # but we don't want them because there are no properties they'd fit in.
    tree = html.fromstring(description)
    sanction_imposed = access_html(tree, "Sanction imposed")
    obligation_breached = access_html(tree, "Obligation breached")
    date_of_infringement_notice = access_html(tree, "Date of infringement notice")
    # We expect at least one of these fields to be present.
    if not any((sanction_imposed, obligation_breached, date_of_infringement_notice)):
        context.log.warn(
            "Suspicious bahaviour of the HTML structure, 'description' filed could be off"
        )
        return

    entity = context.make("Company")
    entity.id = context.make_id(trading_name, abn)
    entity.add("name", trading_name)
    entity.add("alias", sponsor_name)
    entity.add("registrationNumber", abn)
    entity.add("country", "au")
    entity.add("topics", "sanction")
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
    h.apply_date(sanction, "date", date_of_infringement_notice)
    h.apply_date(sanction, "date", item.pop("sanctioninfrinoticedate"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        item,
        ignore=[
            "sanctionimposed",
            "sanctionimposeddate",
            "amountstatus",
            "sanctioninfrinoticeamount",
            "sanctioninfribreachnotice",
        ],
    )


def crawl(context: Context):
    data = context.fetch_json(
        context.data_url,
        cache_days=1,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json;odata=verbose",
        },
        data="{}",
        method="POST",
    )
    # print("Fetched data:", data)
    for item in data.get("d", {}).get("data", []):
        crawl_item(context, item)
