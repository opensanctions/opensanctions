import re
from typing import List
from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML

from zavod import Context, Entity
from zavod import helpers as h

REGEX_DELAY = re.compile(".+(\d{2}[\.\/]\d{2}[\.\/]\d{4})$")
# e.g. 6/23 from "6/23 din 02.05.2023"
REGEX_SANCTION_NUMBER = re.compile(".*(\d+\/\d+)[\w ]+(\d{2}[\.\/]\d{2}[\.\/]\d{4}).*")
REGEX_MEMBER_GROUPS = (
    "^(?P<unknown>[\w, \(\)%\.]+)?"
    "("
    "(ADMINISTRATORS: (?P<admin>[\w,\. ]+))"
    "|OWNERS: ((?P<owners>[\w,\. \(\)%]+))"
    "|UNKNOWN: ((?P<mixed>[\w,\. \(\)%]+))"
    ")*$"
)

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


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.data_url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)

    table = doc.find(".//table")
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [slugify(el.text_content()) for el in row.findall("./th")]
            continue

        cells = [collapse_spaces(el.text_content()) for el in row.findall("./td")]
        data = {hdr: c for hdr, c in zip(headers, cells)}

        entity = context.make("Company")
        name = data.pop("denumirea-si-forma-de-organizare-a-operatorului-economic")
        entity.id = context.make_id(name, "md")
        entity.add("name", name)
        entity.add("topics", "debarment")

        addr_string = data.pop("adresa-si-datele-de-contact-ale-operatorului-economic")
        address = h.make_address(context, full=addr_string, country_code="md")
        h.apply_address(context, entity, address)

        delay_until_date = parse_delay(context, data.pop("mentiuni"))
        start_date = parse_date(data.pop("data-inscrierii"))
        start_date = delay_until_date or start_date

        sanction_num, decision_date = parse_sanction_decision(
            context, data.pop("nr-si-data-deciziei-agentiei")
        )
        sanction = h.make_sanction(context, entity, sanction_num)
        sanction.add("authorityId", sanction_num)
        reason = data.pop(
            "expunerea-succinta-a-temeiului-de-includere-in-lista-a-operatorului-economic"
        )
        sanction.add("reason", reason, lang="ro")
        sanction.add("startDate", start_date)
        sanction.add(
            "endDate", parse_date(data.pop("termenul-limita-de-includere-in-lista"))
        )
        sanction.add("listingDate", start_date)

        owners_and_admins = data.pop("date-privind-administratotul-si-fondatorii")
        crawl_control(context, entity, decision_date, owners_and_admins)

        context.emit(entity, target=True)
        context.emit(sanction)


def parse_delay(context, delay):
    if delay:
        date_match = REGEX_DELAY.match(delay)
        if date_match:
            return parse_date(date_match.group(1))
        else:
            context.log.warn(
                f"Failed to parse date from nonempty delay: { date_match, delay }"
            )


def parse_date(text):
    text = text.lower()
    for ro, en in MONTHS.items():
        text = text.replace(ro, en)
    segments = text.split(", ")
    if len(segments) == 3:
        text = ", ".join(segments[1:])
    return h.parse_date(text, ["%d/%m/%Y", "%d.%m.%Y", "%d %B, %Y"])


def parse_sanction_decision(context, text):
    match = REGEX_SANCTION_NUMBER.match(text)
    if match:
        return match.group(1), parse_date(match.group(2))
    else:
        context.log.warn(f'Failed to parse saction number and date from "{ text }"')
        return None, None


def clean_control_string(text: str) -> str:
    text = text.replace("Lista ", "")
    text = text.replace(" - ", ": ")
    text = text.replace(" \u2013 ", ": ")

    for ro, en in ROLES.items():
        text = text.replace(ro, en)
    return text


def crawl_control(context: Context, entity: Entity, date, text: str):
    entities = []
    text = clean_control_string(text)
    match = re.match(REGEX_MEMBER_GROUPS, text)

    if match:
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

        entities += list(make_ownerships(context, entity, date, owners))
        entities += list(make_members(context, entity, date, members))

    for entity in entities:
        context.emit(entity)


def make_ownerships(context, company: Entity, date, owners: List[str]) -> Entity:
    for name in owners:
        name = name.strip()
        match = re.match("^([^\d\.\()]+)(\(?(\d+\.?\d*) ?%\)?)?$", name)
        if match:
            name = match.group(1)
            owner = context.make("LegalEntity")
            owner.id = context.make_id(company.id, name)
            owner.add("name", name, lang="rum")

            ownership = context.make("Ownership")
            ownership.id = context.make_id(owner.id, "owns", company.id)
            ownership.add("owner", owner)
            ownership.add("asset", company)
            if date:
                ownership.add("date", date)
            percent = match.group(3)
            if percent:
                ownership.add("percentage", percent)

            yield owner
            yield ownership


def make_members(context, company: Entity, date, members: List[str]):
    for name in members:
        name = name.strip()
        if name:
            member = context.make("LegalEntity")
            member.id = context.make_id(company.id, name)
            member.add("name", name, lang="rum")

            membership = context.make("Membership")
            membership.id = context.make_id(member.id, "in", company.id)
            membership.add("member", member)
            membership.add("organization", company)
            if date:
                membership.add("date", date)

            yield member
            yield membership
