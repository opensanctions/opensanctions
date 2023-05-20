from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML
import re

from opensanctions.core import Context
from opensanctions import helpers as h

REGEX_DELAY = re.compile(".+(\d{2}[\.\/]\d{2}[\.\/]\d{4})$")

MONTHS = {
    "ianuarie": "January",
    "februarie": "February",
    "martie": "March",
    "aprilie": "April",
    "mai": "May",
    "iunie": "June",
    "iulie": "July",
    "august": "August",
    "septembrie": "September",
    "octombrie": "October",
    "noiembrie": "November",
    "decembrie": "December",
}

IDX_ORG_NAME = 1
IDX_ORG_ADDRESS = 2
IDX_ADMINS_FOUNDERS = 3
IDX_APPLICANT = 4
IDX_DECISION_NUM_DATE = 5
IDX_REASON = 6
IDX_REGISTRATION_DATE = 7
IDX_DELAY_UNTIL = 8
IDX_END_DATE = 9

COUNTRY = "md"

def crawl(context: Context):
    path = context.fetch_resource("source.html", context.source.data.url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)

    table = doc.find(".//table")
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [c.text_content() for c in row.findall("./th")]

            # Assert that column order is as expected:
            # fail explicitly upon possible breaking change
            assert "Denumirea şi forma" in headers[IDX_ORG_NAME]
            assert "Adresa  şi datele" in headers[IDX_ORG_ADDRESS]
            assert "administratotul si fondatorii" in headers[IDX_ADMINS_FOUNDERS]
            assert "Solicitantul" in headers[IDX_APPLICANT]
            assert "Nr şi data deciziei" in headers[IDX_DECISION_NUM_DATE]
            assert "temeiului de includere" in headers[IDX_REASON]
            assert "Data înscrierii" in headers[IDX_REGISTRATION_DATE]
            assert "Mențiuni" in headers[IDX_DELAY_UNTIL]
            assert "Termenul limită" in headers[IDX_END_DATE]

            continue

        cells = row.findall("./td")
        
        name = cells[IDX_ORG_NAME].text_content()
        
        entity = context.make("Company")
        entity.id = context.make_id(name, COUNTRY)
        entity.add("name", name)
        
        full_address = cells[IDX_ORG_ADDRESS].text_content()
        address = h.make_address(context, full=full_address, country=COUNTRY)
        h.apply_address(context, entity, address)

        delay_until_date = parse_delay(context, cells[IDX_DELAY_UNTIL].text_content().strip())
        start_date = delay_until_date or parse_date(cells[IDX_REGISTRATION_DATE].text_content().strip())
        
        sanction = h.make_sanction(context, entity)
        sanction.add("reason", cells[IDX_REASON].text_content().strip(), lang="ro")
        sanction.add("startDate", start_date)
        sanction.add("endDate", parse_date(cells[IDX_END_DATE].text_content().strip()))

        context.emit(entity, target=True)
        context.emit(sanction)


def parse_delay(context, delay):
    if delay:
        date_match = REGEX_DELAY.match(delay)
        if date_match:
            return parse_date(date_match.group(1))
        else:
            context.log.error(f"Failed to parse date from nonempty delay: { date_match, delay }")


def parse_date(text):
    for ro, en in MONTHS.items():
        text = text.replace(ro, en)
    return h.parse_date(text, ["%d/%m/%Y", "%d.%m.%Y", "%A, %d %B, %Y"])