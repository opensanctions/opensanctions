import csv
import re
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional

import pdfplumber
from lxml import html
from rigour.mime.types import CSV, PDF
from rigour.text.scripts import get_script

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource, fetch_html

SOURCE_URL = "https://www.meti.go.jp/policy/external_economy/trade_control/02_export/17_russia/russia.html"
NAMES_PATTERN = re.compile(
    r"""
    ^\d+\s*                     # Match and ignore leading number and optional whitespace
    (?P<main>[^（）]+?)         # Capture the main name, select lazily
    \s*                         # Allow optional space before parentheses
    （別称、                    # Opening parenthesis with '別称、' indicating the start of aliases
    (?P<aliases>[^）]+)        # Capture aliases content inside the parentheses
    ）                         # Must-have closing parenthesis for valid matches
    $                           # End of line
""",
    re.VERBOSE | re.MULTILINE,
)
TRAILING_WHITESPACE_PATTERN = re.compile(r"\s+$", re.MULTILINE)
LOCAL_PATH = Path(__file__).parent
EXPECTED_HASHES = {
    "list_belarus_tokutei.pdf": "cd99cd520f06110ad39f354d6c961fe5c36260e3",
    "250912_list_russia_tokutei.pdf": "16a38c66fe9a05c3acbda50cdca5e93ca420eb83",
    "250912_list_daisangoku_tokutei.pdf": "1464205ea2708c0348e3a1cff5dbf79c513b672c",
}


def detect_script(context, text: str) -> Optional[str]:
    """Detect dominant script in a string. Return 'jpn' or 'zho' if confident, else None."""
    scripts = [get_script(ord(ch)) for ch in text if get_script(ord(ch))]
    if not scripts:
        context.log.warning(f"Could not detect script for: {text}")
        return None
    # Japanese: Hiragana or Katakana present
    if "Hiragana" in scripts or "Katakana" in scripts:
        return "jpn"
    # English: Latin alphabet present
    if "Latin" in scripts:
        return "eng"
    # Uncertain: Han characters could be Japanese or Chinese
    return None


def split_aliases(context, raw_aliases: str):
    """Split and detect script for a semicolon-separated string of aliases."""
    result = []
    for alias in re.split(r"、|及び|;", raw_aliases):
        alias = alias.strip()
        if alias:
            result.append((alias, detect_script(context, alias)))
    return result


def clean_address(raw_address):
    # Remove the 'location' "所在地：" from the start of the string
    cleaned = re.sub(r"^所在地[:：]", "", raw_address).strip()
    # Split into parts by common delimiters
    parts = h.multi_split(cleaned, [" and ", ";"])
    return [p.strip(" ,;") for p in parts if p.strip()]


def clean_name_en(data_string):
    # Split the string to separate the primary name from aliases
    if "a.k.a." in data_string:
        parts = data_string.split("a.k.a.")
    elif "the following" in data_string:
        parts = data_string.split("the following")
    else:
        # Default: Return the data string as the name with no aliases
        return data_string.strip(), []

    clean_name = parts[0].strip().rstrip(",").rstrip(".").rstrip("a.k.a")
    aliases = []
    if len(parts) > 1:
        aliases_part = parts[1]
        aliases = re.findall(r"[-—]([^;\n]+)(?:;|\.|\n| and |$)", aliases_part)
        aliases = [alias.strip().rstrip(".").rstrip(",") for alias in aliases]

    return clean_name, aliases


def clean_name_raw(context, name_jpn, row):
    main_name = ""
    aliases = []

    # Attempt to match the entire name
    match = NAMES_PATTERN.fullmatch(name_jpn)
    if match:
        main_name = match.group("main").strip()
        aliases_raw = match.group("aliases") or ""
        aliases.extend(split_aliases(context, aliases_raw))
    # Check for cases that require manual processing
    if main_name == "" and not aliases:
        # Identify complex cases with certain characters
        if any(char in name_jpn for char in ["（", "、", ")", "）"]):
            main_name = row.pop("name_jpn_cleaned")
            aliases_raw = row.pop("aliases_jpn_cleaned")
            aliases.extend(split_aliases(context, aliases_raw))
        else:
            # Remove leading numbers for cases with names only
            # (e.g. '4 株式会社コンペル')
            name_jpn = re.sub(r"^\d+\s*", "", name_jpn)
            # Use cleaned fields if available
            cleaned_name = row.pop("name_jpn_cleaned", "").strip()
            aliases_raw = row.pop("aliases_jpn_cleaned", "").strip()
            main_name = cleaned_name if cleaned_name else name_jpn.strip()
            aliases.extend(split_aliases(context, aliases_raw))

    if not main_name:
        context.log.warning(
            f"Entry needs manual processing: {name_jpn}. Please fix in the Google Sheet."
        )

    return main_name, aliases


def crawl_row(context, row):
    name_jpn = row.pop("name_raw")
    name_en = row.pop("name_en")
    address = clean_address(row.pop("address"))

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name_jpn, name_en)
    # Japanese name and alias cleanup
    name_jpn_clean, aliases_jpn_zho = clean_name_raw(context, name_jpn, row)
    entity.add("name", name_jpn_clean, lang="jpn")
    for alias, lang in aliases_jpn_zho:
        entity.add("alias", alias, lang=lang)
    # English name and alias cleanup
    name_en_clean, aliases = clean_name_en(name_en)
    entity.add("name", name_en_clean, lang="eng")
    for alias in aliases:
        entity.add("alias", alias, lang="eng")
    for address in h.multi_split(address, [" and "]):
        entity.add("address", address)

    entity.add("sourceUrl", row.pop("source_url"))
    entity.add("topics", "export.control")
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("program", row.pop("program"))
    h.apply_date(sanction, "listingDate", row.pop("designated_date"))
    h.apply_date(sanction, "modifiedAt", row.pop("last_updated"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        row,
        [
            "target_country",
            "auto-translated",
        ],
    )


def crawl(context: Context):
    divs_xpath = ".//div[@class='wrapper2011']"
    doc = fetch_html(
        context,
        SOURCE_URL,
        divs_xpath,
        html_source="httpResponseBody",
        geolocation="jp",
        absolute_links=True,
    )
    divs = doc.xpath(divs_xpath)
    assert len(divs) == 1, len(divs)
    # Check hash of the content part of the page
    h.assert_dom_hash(divs[0], "982832a856dfe254b6282966ec96cfb58d9464aa")
    pdf_xpath = ".//a[contains(@href, '.pdf') and contains(@href, 'export/17_russia/') and contains(@href, 'tokutei')]/@href"
    pdf_urls = divs[0].xpath(pdf_xpath)
    assert len(pdf_urls) == 3, len(pdf_urls)

    # Update local copy of just the content part of the page to diff easily when
    # there are changes. Commit changes once they're handled.
    with open(LOCAL_PATH / "page_content.txt", "w") as fh:
        text = html.tostring(
            divs[0],
            pretty_print=True,
            method="text",
            encoding="utf-8",
        ).decode("utf-8")
        text = TRAILING_WHITESPACE_PATTERN.sub("", text)
        fh.write(text)

    for pdf_url in pdf_urls:
        pdf_name = Path(urlparse(pdf_url).path).name
        _, _, _, pdf_path = fetch_resource(
            context, pdf_name, pdf_url, expected_media_type=PDF, geolocation="jp"
        )
        h.assert_file_hash(pdf_path, EXPECTED_HASHES.get(pdf_name))

        # Save the text of the PDFs linked to from the page for easy diffing.
        # Commit changes once they're handled.
        pdf_text = ""
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                pdf_text += page.extract_text()
        pdf_text_path = LOCAL_PATH / f"{pdf_name}.txt"
        with open(pdf_text_path, "w") as fh:
            fh.write(pdf_text)

    # Crawling the google sheet
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
