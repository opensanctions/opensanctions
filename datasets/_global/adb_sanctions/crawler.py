from typing import Dict
import re

from zavod import Context
from zavod import helpers as h

REG_NRS = ["(Reg. No:", "(Reg. No.:", "(Reg. No.", "(Trade Register No.:"]
NAME_SPLITS = [
    "; ",
    "also known as",
    "also doing business as",
    "formerly operating as",
    "also",
    "formerly",
    "f/k/a",
    "(AKA",
]
# MIRROR_URL = "https://data.opensanctions.org/contrib/adb_sanctions/data.html"
REGEX_ALIAS_REGNO = re.compile(
    r"(?P<name>.{5,30})[;,] (Registration no.|ID:) (?P<regno>.{5,20})", re.IGNORECASE
)


def crawl_row(context: Context, row: Dict[str, str | None]):
    full_name = row.pop("name") or ""

    # Split the full name using NAME_SPLITS first
    name_parts = h.multi_split(full_name, NAME_SPLITS)

    for part in name_parts:
        name_optional_regno = part
        registration_number = None

        # Further split each part using REG_NRS
        for splitter in REG_NRS:
            if splitter in part:
                part, registration_number = part.split(splitter, 1)
                registration_number = registration_number.replace(")", "").strip()
                break

        country = row.get("nationality") or ""
        country = country.replace("Non ADB Member Country", "")
        country = country.replace("Rep. of", "​").strip()

        entity = context.make("LegalEntity")
        entity.id = context.make_id(name_optional_regno, country)
        entity.add("name", part)

        # Handle missing 'othername_logo' key gracefully
        other_names = row.get("othername_logo").replace("\\", "")
        if match := REGEX_ALIAS_REGNO.match(other_names):
            entity.add("alias", match.group("name"))
            entity.add("registrationNumber", match.group("regno"))
        elif ":" in other_names or "no." in other_names.lower():
            res = context.lookup("other_names", other_names)
            if res:
                for item in res.items:
                    entity.add(item["prop"], item["value"])
            else:
                context.log.warning("Unhandled other_names", value=other_names)
        else:
            entity.add("alias", other_names)

        entity.add("topics", "debarment")
        entity.add("country", country)
        entity.add("registrationNumber", registration_number)

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", row.get("grounds"))
        sanction.add("program", row.get("sanction_type"))

        date_range = row.get("effect_date_lapse_date", "") or ""
        if "|" in date_range:
            start_date, end_date = date_range.split("|")
            h.apply_date(sanction, "startDate", start_date.strip())
            h.apply_date(sanction, "endDate", end_date.strip())

        address = h.make_address(context, full=row.get("address"), country=country)

        h.apply_address(context, entity, address)
        context.emit(entity, target=True)
        context.emit(sanction)


def crawl(context: Context):
    url = None
    next_url = context.data_url
    next_xpath = ".//a//*[text() = 'Next Page »»']/parent::*/parent::a"
    pages = 0
    while url != next_url:
        url = next_url
        doc = context.fetch_html(url)
        doc.make_links_absolute(url)
        next_url = doc.xpath(next_xpath)[0].get("href")

        print(next_url)

        table = doc.find(".//div[@id='viewcontainer']/table")

        for row in h.parse_html_table(table):
            crawl_row(context, h.cells_to_str(row))

        pages += 1
        assert pages <= 10, "More pages than expected."
