from typing import Dict
from normality import collapse_spaces
from rigour.mime.types import PDF
from urllib.parse import urljoin
import re

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource


REGEX_NON_ASCII_PARENS = re.compile(r"([\(（]\w*[^a-zA-Z\)）]{3,}[\)）])")
REGEX_NON_ASCII = re.compile(r"[^ a-zA-Z'-]+")
NAME_REPLACEMENTS = [
    ("（", "("),
    ("）", ")"),
    ("，", ","),
    ("・", ""),
]


def crawl_pdf_url(context: Context) -> str:
    validator = ".//a[contains(text(), 'End User List')]"
    html = fetch_html(context, context.data_url, validator)
    for a in html.findall(".//a"):
        if a.text is not None and "Review of the End User List" in a.text:
            review_url = urljoin(context.data_url, a.get("href"))
            html = fetch_html(context, review_url, validator)
            for a in html.findall(".//a"):
                if a.text is None or "End User List" not in a.text:
                    continue
                if ".pdf" in a.get("href", ""):
                    return urljoin(context.data_url, a.get("href"))
    raise ValueError("No PDF found")


def english_headers(row: Dict[str, str]) -> Dict[str, str]:
    new_row = {}
    for key, value in row.items():
        parts = key.split("\n")
        if len(parts) == 1:
            new_key = parts[0]
        elif len(parts) == 2:
            new_key = parts[1]
        else:
            raise Exception("Unexpected header", header=key, row=row)
        new_row[new_key] = value
    return new_row


def crawl(context: Context):
    pdf_url = crawl_pdf_url(context)
    _, _, _, path = fetch_resource(
        context, "source.pdf", pdf_url, expected_media_type=PDF
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    last_no = 0
    for holder in h.parse_pdf_table(context, path, preserve_header_newlines=True):
        holder = english_headers(holder)
        no = int(holder.pop("no"))
        if no != last_no + 1:
            context.log.warn(
                "Row number is not continuous",
                no=no,
                last_no=last_no,
            )
        last_no = no
        name = collapse_spaces(holder.pop("company_or_organization")).strip()
        non_ascii_match = REGEX_NON_ASCII_PARENS.search(name)
        if non_ascii_match:
            name_jpn = non_ascii_match.group(1)
            name = name.replace(name_jpn, "").strip()
            name_jpn = name_jpn.strip("()（）")
        else:
            name_jpn = None
        for orig, repl in NAME_REPLACEMENTS:
            name = name.replace(orig, repl)

        aliases = [collapse_spaces(a) for a in holder.pop("also_known_as").split("・")]

        country = collapse_spaces(holder.pop("country_or_region"))
        country = REGEX_NON_ASCII.sub("", country).strip()

        entity = context.make("LegalEntity")
        entity.id = context.make_id(str(no), name)
        entity.add("name", name, lang="eng")
        entity.add("name", name_jpn, lang="jpn")
        entity.add("alias", aliases)
        entity.add("country", country)
        entity.add("topics", "sanction")

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", h.multi_split(holder.pop("type_of_wmd"), ",、\n"))
        context.emit(entity)
        context.emit(sanction)

        context.audit_data(holder)
