from rigour.mime.types import PDF
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.shed.gpt import run_image_prompt
from zavod.shed.zyte_api import fetch_html, fetch_resource

prompt = """
Extract structured data from the following page of a PDF document.
This PDF contains a table on each page where values can wrap onto the
next line within a cell, and values are centred vertically in the cell.
Country is first in japanese and then in latin script on the next line.
Company name is always in latin script, possibly wrapping onto the next line,
and sometimes in japanese script on another line. Include all hyphenated parts
of the name. Records are separated by a horizontal line.
Return a JSON list (`rows`) in which each object represents a row in the table.
Each object should have the following fields: `no` (string), `country`
(latin script version only), `name` (company or organization name in latin
script), `name_jpn` (company or organization name in japanese script, or
empty), `also_known_as` (an array with a string for each bullet point)
`type_of_wmd` (an array with the letters indicated).
Return an empty string for undefined fields.
"""


def unblock_validator(el) -> bool:
    return "End User List" in el.text_content()


def crawl_pdf_url(context: Context) -> str:
    html = fetch_html(context, context.data_url, unblock_validator, cache_days=1)
    for a in html.findall(".//a"):
        if a.text is not None and "Review of the End User List" in a.text:
            review_url = urljoin(context.data_url, a.get("href"))
            html = fetch_html(context, review_url, unblock_validator, cache_days=1)
            for a in html.findall(".//a"):
                if a.text is None or "End User List" not in a.text:
                    continue
                if ".pdf" in a.get("href", ""):
                    return urljoin(context.data_url, a.get("href"))
    raise ValueError("No PDF found")


def crawl(context: Context):
    pdf_url = crawl_pdf_url(context)
    cached, path, media_type, charset = fetch_resource(context, "source.pdf", pdf_url)
    if not cached:
        assert media_type == PDF, media_type
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    last_no = 0
    for page_path in h.make_pdf_page_images(path):
        data = run_image_prompt(context, prompt, page_path)
        assert "rows" in data, data
        for holder in data.get("rows", []):
            no = int(holder.get("no"))
            if no != last_no + 1:
                context.log.warn(
                    "Row number is not continuous",
                    no=no,
                    last_no=last_no,
                    url=pdf_url,
                )
            last_no = no
            name = holder.get("name")
            entity = context.make("LegalEntity")
            entity.id = context.make_id(str(no), name)
            entity.add("name", name, lang="eng")
            entity.add("name", holder.get("name_jpn"), lang="jpn")
            entity.add("alias", holder.get("also_known_as"))
            entity.add("country", holder.get("country"))
            entity.add("topics", "sanction")

            sanction = h.make_sanction(context, entity)
            sanction.add("reason", holder.get("type_of_wmd"))
            context.emit(entity, target=True)
            context.emit(sanction)
