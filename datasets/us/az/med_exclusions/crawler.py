from typing import Dict
import ast
import json
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

    context.emit(entity, target=True)
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

    for i, item in enumerate(items):

        if (
            (not item.get("name_provider"))
            or (len(item.get("name_provider").split("\n")) > 1)
            or (len(item.get("action_type_suspend_terminate").split("\n")) > 1)
        ):

            manual_extraction[i] = True
            if i != 0:
                manual_extraction[i - 1] = True

    for i, item in enumerate(items):
        if not manual_extraction[i]:
            crawl_item(item, context)
        else:
            if i != 0:
                joined_string = slugify(stringify(items[i - 1 : i + 1]))
            else:
                joined_string = slugify(stringify(items[i : i + 1]))

            corrected_items = context.lookup_value("manual_extraction", joined_string)

            if not corrected_items:
                context.log.warning("Unable to parse: " + joined_string)
                continue

            corrected_items = ast.literal_eval(corrected_items)

            for corrected_item in corrected_items:
                crawl_item(corrected_item, context)
