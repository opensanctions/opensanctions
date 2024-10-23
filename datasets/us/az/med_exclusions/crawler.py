from pathlib import Path
from typing import Dict
from rigour.mime.types import PDF

import pdfplumber
from normality import collapse_spaces, slugify
from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource


def parse_pdf_table(
    context: Context, path: Path, save_debug_images=False, headers=None
):
    pdf = pdfplumber.open(path.as_posix())
    settings = {}
    for page_num, page in enumerate(pdf.pages, 1):
        # Find the bottom of the bottom-most rectangle on the page
        bottom = max(page.height - rect["y0"] for rect in page.rects)
        settings["explicit_horizontal_lines"] = [bottom]
        if save_debug_images:
            im = page.to_image()
            im.draw_hline(bottom, stroke=(0, 0, 255), stroke_width=1)
            im.draw_rects(page.find_table(settings).cells)
            im.save(f"page-{page_num}.png")
        assert bottom < (page.height - 5), (bottom, page.height)

        for row in page.extract_table(settings)[1:]:
            if headers is None:
                headers = [slugify(collapse_spaces(cell), sep="_") for cell in row]
                continue
            assert len(headers) == len(row), (headers, row)
            yield dict(zip(headers, row))


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
    cached, path, media_type, charset = fetch_resource(
        context, "source.pdf", context.data_url, geolocation="us"
    )
    if not cached:
        assert media_type == PDF, media_type
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    previous_item = None
    for item in parse_pdf_table(context, path):

        # There are some cases where the name is same but they have multiple NPIs
        if not item.get("name_provider"):
            for key in item.keys():
                if not item.get(key):
                    item[key] = previous_item[key]

        previous_item = item.copy()

        # If there are multiple providers in the same dictionary we are going to split into two
        if (
            item.get("name_provider")
            and len(item.get("name_provider").split("\n")) > 1
            and len(item.get("action_type_suspend_terminate").split("\n")) > 1
        ):
            item1, item2 = {}, {}

            for key, value in item.items():
                parts = value.split("\n")
                # There are some cases there is multiple values and others
                # where the providers share the same value
                if len(parts) == 2:
                    item1[key] = parts[0].strip()
                    item2[key] = parts[1].strip()
                elif len(parts) == 1:
                    item1[key] = parts[0].strip()
                    item2[key] = parts[0].strip()
                else:
                    context.log.warning("Unable to parse: ", item)

            crawl_item(item1, context)
            crawl_item(item2, context)
        else:
            crawl_item(item, context)
