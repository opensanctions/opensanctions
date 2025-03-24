import csv
import re
from rigour.mime.types import CSV

from zavod import Context, helpers as h

SOURCE_URL = "https://www.meti.go.jp/policy/external_economy/trade_control/02_export/17_russia/russia.html"
HEADER = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
}
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


def clean_address(raw_address):
    # Remove the 'location' "所在地：" from the start of the string
    cleaned_address = re.sub(r"^所在地：", "", raw_address).strip()
    cleaned_address = re.sub(r"^所在地:", "", cleaned_address).strip()
    return cleaned_address


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
        aliases = [
            alias.strip()
            for alias in re.split(r"、|及び", aliases_raw)
            if alias.strip()
        ]
    # Check for cases that require manual processing
    if main_name == "" and not aliases:
        # Identify complex cases with certain characters
        if any(char in name_jpn for char in ["（", "、", ")", "）"]):
            main_name = row.pop("name_jpn_cleaned")
            aliases_raw = row.pop("aliases_jpn_cleaned")
            aliases = [
                alias.strip() for alias in aliases_raw.split(";") if alias.strip()
            ]
        else:
            # Remove leading numbers for cases with names only
            # (e.g. '4 株式会社コンペル')
            name_jpn = re.sub(r"^\d+\s*", "", name_jpn)
            main_name = name_jpn

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
    name_jpn_clean, aliases_jpn = clean_name_raw(context, name_jpn, row)
    entity.add("name", name_jpn_clean, lang="jpn")
    for alias in aliases_jpn:
        entity.add("alias", alias, lang="jpn")
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
    # Checking whether the hash of the page has changed
    doc = context.fetch_html(SOURCE_URL, headers=HEADER)
    dom = doc.xpath(".//div[@class='wrapper2011']")
    assert len(dom) == 1, f"Too many divs: {len(dom)}"
    h.assert_dom_hash(dom[0], "43ed0739eb39b8a87c87d10a8d353ccdf0ebf2cb")
    # Crawling the google sheet
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
