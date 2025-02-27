from typing import Dict
from rigour.mime.types import PDF

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource
from normality import slugify, stringify


def crawl_item(row: Dict[str, str], context: Context):

    if not row.get("name_provider") and not row.get("npi"):
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(row.get("name_provider"), row.get("npi"))
    entity.add("name", row.pop("name_provider"))
    if row.get("npi") not in ["NONE", "No NPI"]:
        entity.add("npiCode", row.pop("npi"))
    else:
        row.pop("npi")
    entity.add("topics", "debarment")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    sanction.add("provisions", row.pop("action_type_suspend_terminate"))
    h.apply_date(sanction, "startDate", row.pop("effective_date"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:

    _, _, _, path = fetch_resource(
        context,
        "source.pdf",
        context.data_url,
        expected_media_type=PDF,
        geolocation="us",
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    items = list(h.parse_pdf_table(context, path, skiprows=1))
    manual_extraction = [False for _ in items]

    # Mark rows that look like they might need manual extraction,
    # as well as one row before and after.
    for i, item in enumerate(items):
        if (
            (not item.get("name_provider"))
            or "\n" in item.get("name_provider")
            or "\n" in item.get("action_type_suspend_terminate")
        ):
            manual_extraction[i] = True
            if i != 0:
                manual_extraction[i - 1] = True
            if i != len(items) - 1:
                manual_extraction[i + 1] = True

    rows = []
    manual_strings = []
    last_manual = manual_extraction[0]
    if manual_extraction[0]:
        manual_strings.append(slugify(stringify(items[0])))

    # concatenate contiguous manual-extraction rows
    rows.append(items[0])
    for i, item in enumerate(items[1:], 1):
        if manual_extraction[i]:
            string = slugify(stringify(item))
            if last_manual:
                manual_strings[-1] += "\n" + string
            else:
                manual_strings.append(string)
        else:
            rows.append(item)
        last_manual = manual_extraction[i]

    # Look up manual extraction row values
    for string in manual_strings:
        res = context.lookup("manual_extraction", string)
        if not res:
            context.log.warning("Unable to parse: " + string)
            continue
        rows.extend(res.rows)

    for row in rows:
        crawl_item(row, context)
