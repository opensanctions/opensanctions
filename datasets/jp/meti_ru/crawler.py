import csv
import re
from rigour.mime.types import CSV

from zavod import Context, helpers as h

SOURCE_URL = "https://www.meti.go.jp/policy/external_economy/trade_control/02_export/17_russia/russia.html"
HEADER = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
}


def clean_address(raw_address):
    # Remove the 'location' "所在地：" from the start of the string
    cleaned_address = re.sub(r"^所在地：", "", raw_address).strip()
    cleaned_address = re.sub(r"^所在地:", "", cleaned_address).strip()
    return cleaned_address


def clean_up_name(data_string):
    # Split the string to separate the primary name from aliases
    if ", a.k.a." in data_string:
        parts = data_string.split(", a.k.a.")
    elif "the following" in data_string:
        parts = data_string.split("the following")
    else:
        # Default: Return the data string as the name with no aliases
        return data_string.strip(), []

    clean_name = parts[0].strip().rstrip(",")
    aliases = []
    if len(parts) > 1:
        aliases_part = parts[1]
        aliases = re.findall(r"[-—]([^;\n]+)(?:;|\.|\n| and |$)", aliases_part)
        aliases = [alias.strip() for alias in aliases]

    return clean_name, aliases


def clean_japanese_names(data_string):
    entries = re.split(r"(?<=\w)(?=\d{1,3}\s)", data_string)

    for entry in entries:
        entry = re.sub(r"^\d{1,3}\s+", "", entry).strip()
        # Separate main name and alias part
        if "（別称、" in entry:
            parts = entry.split("（別称、")
            main_name = parts[0].strip()
            # Ensure there is an alias part to process
            if len(parts) > 1:
                alias_part = parts[1].rstrip("）")
                # Split aliases using "、" and "及び"
                aliases = re.split(r"、|及び", alias_part)
                aliases = [alias.strip() for alias in aliases if alias.strip()]
            else:
                aliases = []
        else:
            main_name = entry.strip()
            aliases = []

    return main_name, aliases


def crawl_row(context, row):
    name_jap = row.pop("name_raw")
    name_en = row.pop("name_en")
    address = clean_address(row.pop("address"))
    entity = context.make("LegalEntity")
    entity.id = context.make_id(name_jap, name_en)
    # Japanese name and alias cleanup
    name_jap_clean, aliases = clean_japanese_names(name_jap)
    entity.add("name", name_jap_clean, lang="jpn")
    for alias in aliases:
        entity.add("alias", alias, lang="jpn")
    # English name and alias cleanup
    name_en_clean, aliases = clean_up_name(name_en)
    entity.add("name", name_en_clean)
    for alias in aliases:
        entity.add("alias", alias, lang="eng")
    for address in h.multi_split(address, [" and "]):
        entity.add("address", address)

    entity.add("sourceUrl", row.pop("source_url"))
    entity.add("topics", "debarment")
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("program", row.pop("program"))
    h.apply_date(sanction, "listingDate", row.pop("designated_date"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ["target_country"])


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
