from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML
import re

from opensanctions.core import Context
from opensanctions import helpers as h

REGEX_DELAY = re.compile(".+(\d{2}[\.\/]\d{2}[\.\/]\d{4})$")
# e.g. 6/23 from "6/23 din 02.05.2023" 
REGEX_SANCTION_NUMBER = re.compile(".*(\d+/\d+) .+")

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
        
        address = parse_address(context, cells[IDX_ORG_ADDRESS].text_content())
        h.apply_address(context, entity, address)

        delay_until_date = parse_delay(context, cells[IDX_DELAY_UNTIL].text_content().strip())
        start_date = delay_until_date or parse_date(cells[IDX_REGISTRATION_DATE].text_content().strip())
        
        sanction_num = parse_sanction_num(context, cells[IDX_DECISION_NUM_DATE].text_content().strip())
        sanction = h.make_sanction(context, entity, sanction_num)
        sanction.add("authorityId", sanction_num)
        sanction.add("reason", cells[IDX_REASON].text_content().strip(), lang="ro")
        sanction.add("startDate", start_date)
        sanction.add("endDate", parse_date(cells[IDX_END_DATE].text_content().strip()))
        sanction.add("listingDate", parse_date(cells[IDX_REGISTRATION_DATE].text_content().strip()))

        parse_control(context, cells[IDX_ADMINS_FOUNDERS].text_content().strip())

        # Persons


        # Ownerships

        # Memberships

        

        context.emit(entity, target=True)
        context.emit(sanction)


def parse_delay(context, delay):
    if delay:
        date_match = REGEX_DELAY.match(delay)
        if date_match:
            return parse_date(date_match.group(1))
        else:
            context.log.warn(f"Failed to parse date from nonempty delay: { date_match, delay }")


def parse_date(text):
    for ro, en in MONTHS.items():
        text = text.replace(ro, en)
    return h.parse_date(text, ["%d/%m/%Y", "%d.%m.%Y", "%A, %d %B, %Y"])


def parse_sanction_num(context, text) -> str | None:
    match = REGEX_SANCTION_NUMBER.match(text)
    if match:
        return match.group(1)
    else:
        context.log.warn(f'Failed to parse saction number from "{ text }"')


def parse_address(context, text):
    # mun. Chişinău, Durleşti, str-la Codrilor 22/1, of. 92, MD-2003
    # or
    # r. Străşeni, s. Micăuţi, MD-3722
    #
    # Stradă (str.) -> street
    # stradelă (str-lă) -> lane
    # soseaua ((şos.) -> road
    # mun. Chişinău -> Chişinău municipality
    # Bulevard (bd.) -> boulevard
    # Căsuţa poştăla (C.P.) -> PO Box
    address = h.make_address(context, full=text, country=COUNTRY)
    return address


def parse_control(context: Context, text: str):
    text = text.replace("Lista ", "")
    text = text.replace(" - ", ": ")
    text = text.replace(" \u2013 ", ": ")
    text = re.sub("\s+", " ", text)
    # Fondator/Administrator – 
    # or
    # Fondator - TOLMAŢCHI VALERI Administrator - TOLMAŢCHI VALERI
    # or
    # Lista conducătorilor - Galin Anatolie, Lista fondatorilor - Galin Anatolie
    # (List of leaders)
    # or
    # Conducători -
    # (Leaders)
    ROLES = { 
        "Fondator/Administrator": "UNKNOWN",
        "conducătorilor": "ADMINISTRATORS",
        "Conducători": "ADMINISTRATORS",
        "Fondator": "OWNERS",
        "fondatorilor": "OWNERS",
        "Administrator": "ADMINISTRATORS",
    }

    for ro, en in ROLES.items():
        text = text.replace(ro, en)

    REGEX_MEMBER_GROUPS = (
        "^(?P<unknown>[\w, \(\)%\.]+)?"
        "("
        "(ADMINISTRATORS: (?P<admin>[\w,\. ]+))"
        "|OWNERS: ((?P<owners>[\w,\. \(\)%]+))"
        "|UNKNOWN: ((?P<mixed>[\w,\. \(\)%]+))"
        ")*$"
    )
    match = re.match(REGEX_MEMBER_GROUPS, text)
    
    print()
    print(text)
    if match:
        #print(f"    -> { match.groupdict()} ")

        owners_str = match.groupdict()["owners"]
        if owners_str:
            owners = owners_str.split(",")
        else:
            owners = []

        members = []
        admins_str = match.groupdict()["admin"]
        if admins_str:
            members = admins_str.split(",")

        unknown_str = match.groupdict()["unknown"]
        if unknown_str:
            for unknown in unknown_str.split(","):
                if "%" in unknown:
                    owners.append(unknown)
                else:
                    members.append(unknown)   

        mixed_str = match.groupdict()["mixed"]
        if mixed_str:
            for unknown in mixed_str.split(","):
                if "%" in unknown:
                    owners.append(unknown)
                else:
                    members.append(unknown)                

        print(f"  owners: { owners }")
        print(f"  members: { members }")