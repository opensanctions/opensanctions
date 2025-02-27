import re
from typing import List
from lxml import html
from rigour.mime.types import HTML

from zavod import Context, Entity
from zavod import helpers as h

REGEX_DELAY = re.compile(r".+(\d{2}[\.\/]\d{2}[\.\/]\d{4})$")
# e.g. 6/23 from "6/23 din 02.05.2023"
REGEX_SANCTION_NUMBER = re.compile(r".*(\d+\/\d+)[\w ]+(\d{2}[\.\/]\d{2}[\.\/]\d{4}).*")
REGEX_MEMBER_GROUPS = (
    r"^(?P<unknown>[\w, \(\)%\.]+)?"
    r"("
    r"(ADMINISTRATORS: (?P<admin>[\w,\. ]+))"
    r"|OWNERS: ((?P<owners>[\w,\. \(\)%]+))"
    r"|UNKNOWN: ((?P<mixed>[\w,\. \(\)%]+))"
    r")*$"
)

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
    for row in h.parse_html_table(table):
        row = h.cells_to_str(row)
        entity = context.make("Company")
        name = row.pop("denumirea_si_forma_de_organizare_a_operatorului_economic")
        entity.id = context.make_id(name, "md")
        entity.add("name", name)
        entity.add("topics", "debarment")

        addr_string = row.pop("adresa_si_datele_de_contact_ale_operatorului_economic")
        address = h.make_address(context, full=addr_string, country_code="md")
        h.apply_address(context, entity, address)

        delay_until_date = None
        delay_note = None
        delay = row.pop("mentiuni")
        if delay:
            date_match = REGEX_DELAY.match(delay)
            if date_match:
                delay_until_date = parse_date(date_match.group(1), context)
            else:
                delay_note = "Mențiuni: " + delay
        start_date = parse_date(row.pop("data_inscrierii"), context)
        start_date = delay_until_date or start_date

        sanction_num, decision_date = parse_sanction_decision(
            context, row.pop("nr_si_data_deciziei_agentiei")
        )
        sanction = h.make_sanction(context, entity, sanction_num)
        sanction.add("authorityId", sanction_num)
        reason = row.pop(
            "expunerea_succinta_a_temeiului_de_includere_in_lista_a_operatorului_economic"
        )
        sanction.add("reason", reason)
        h.apply_date(sanction, "startDate", start_date)
        h.apply_date(
            sanction,
            "endDate",
            parse_date(row.pop("termenul_limita_de_includere_in_lista"), context),
        )
        h.apply_date(sanction, "listingDate", start_date)
        sanction.add("status", delay_note)

        owners_and_admins = row.pop("date_privind_administratotul_si_fondatorii")
        crawl_control(context, entity, decision_date, owners_and_admins)

        context.emit(entity)
        context.emit(sanction)


def parse_date(text: str, context: Context):
    segments = text.split(", ")
    if len(segments) == 3:
        text = " ".join(segments[1:])
    date_info = text
    if date_info:
        return date_info

    context.log.warning("Failed to parse date", raw_date=text)
    return None


def parse_sanction_decision(context, text):
    match = REGEX_SANCTION_NUMBER.match(text)
    if match:
        return match.group(1), parse_date(match.group(2), context)
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
        match = re.match(r"^([^\d\.\()]+)(\(?(\d+\.?\d*) ?%\)?)?$", name)
        if match:
            name = match.group(1)
            owner = context.make("LegalEntity")
            owner.id = context.make_id(company.id, name)
            owner.add("name", name)

            ownership = context.make("Ownership")
            ownership.id = context.make_id(owner.id, "owns", company.id)
            ownership.add("owner", owner)
            ownership.add("asset", company)
            if date:
                h.apply_date(ownership, "date", date)
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
            member.add("name", name)

            membership = context.make("Membership")
            membership.id = context.make_id(member.id, "in", company.id)
            membership.add("member", member)
            membership.add("organization", company)
            if date:
                h.apply_date(membership, "date", date)

            yield member
            yield membership
