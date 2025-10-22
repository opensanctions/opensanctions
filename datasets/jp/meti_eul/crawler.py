import re
from typing import Dict, Optional
from urllib.parse import urljoin

from normality import squash_spaces
from rigour.mime.types import PDF
from zavod.shed.zyte_api import fetch_html, fetch_resource

from zavod import Context
from zavod import helpers as h

REGEX_NON_ASCII_PARENS = re.compile(r"([\(（]\w*[^a-zA-Z\)）]{3,}[\)）])")
REGEX_NON_ASCII = re.compile(r"[^ a-zA-Z'-]+")
NAME_REPLACEMENTS = [
    ("（", "("),
    ("）", ")"),
    ("，", ","),
    ("・", ""),
]

HEADERS = {
    "no": "no",
    "guo_ming_de_yu_ming\ncountry_or_region": "country_or_region",
    "qi_ye_ming_zu_zhi_ming\ncompany_or_organization": "company_or_organization",
    "bie_ming\nalso_known_as": "also_known_as",
    "xuan_nian_qu_fen\ntype_of_wmd": "type_of_wmd",
    "tong_chang_bing_qi\nconventional\nweapons": "conventional_weapons",
}


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


def transform_headers(row: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    if HEADERS.keys() != row.keys():
        raise Exception(f"Unexpected headers {row.keys()} in row {row!r}")
    return {HEADERS[key]: value for key, value in row.items()}


def crawl(context: Context) -> None:
    pdf_url = crawl_pdf_url(context)
    _, _, _, path = fetch_resource(
        context, "source.pdf", pdf_url, expected_media_type=PDF
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    last_no = 0
    for raw_row in h.parse_pdf_table(context, path, preserve_header_newlines=True):
        row = {k: v or "" for k, v in transform_headers(raw_row).items()}
        no = int(row.pop("no"))
        if no != last_no + 1:
            context.log.warn(
                "Row number is not continuous",
                no=no,
                last_no=last_no,
            )
        last_no = no
        name = squash_spaces(row.pop("company_or_organization")).strip()
        non_ascii_match = REGEX_NON_ASCII_PARENS.search(name)
        if non_ascii_match:
            name_jpn = non_ascii_match.group(1)
            name = name.replace(name_jpn, "").strip()
            name_jpn = name_jpn.strip("()（）")
        else:
            name_jpn = None
        for orig, repl in NAME_REPLACEMENTS:
            name = name.replace(orig, repl)

        aliases = [squash_spaces(a) for a in row.pop("also_known_as").split("・")]

        country = squash_spaces(row.pop("country_or_region"))
        country = REGEX_NON_ASCII.sub("", country).strip()

        entity = context.make("LegalEntity")
        entity.id = context.make_id(str(no), name)
        entity.add("name", name, lang="eng")
        entity.add("name", name_jpn, lang="jpn")
        entity.add("alias", aliases)
        entity.add("country", country)
        entity.add("topics", "sanction")

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", h.multi_split(row.pop("type_of_wmd"), ",、\n"))
        context.emit(entity)
        context.emit(sanction)

        context.audit_data(
            row,
            ignore=[
                # Contains "CW" if the row is (also) about conventional weapons
                "conventional_weapons"
            ],
        )
