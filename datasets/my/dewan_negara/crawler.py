import re
from typing import Iterator

import urllib3

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.stateful.review import assert_all_accepted

# The parliament portal serves an incomplete TLS certificate chain, which makes
# the default `requests` verification fail. Disabling verification is acceptable
# here: the source is a public government site and there is no login or secret.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Minimum roster size below which the roster page is assumed to be broken.
DEWAN_NEGARA_MIN = 40
TOPICS = ["gov.legislative", "gov.national"]

# The `Position in the Parliament` field carries the (Malay) presiding-officer
# role labels; ordinary senators and cabinet titles are handled elsewhere.
OFFICER_POSITIONS: dict[str, tuple[str, list[str], str | None]] = {
    "Yang di-Pertua Dewan Negara": (
        "President of the Dewan Negara",
        TOPICS,
        "Q7241319",
    ),
    "Timbalan Yang di-Pertua Dewan Negara": (
        "Deputy President of the Dewan Negara",
        TOPICS,
        "Q134572656",
    ),
}


def parse_detail(context: Context, url: str) -> dict[str, str]:
    """Return the label -> value pairs of a senator's `MAKLUMAT` info table.

    `lang=en` yields English field labels ("Name", "Term of Office", ...); the
    detail links on the roster page omit it, so it is requested explicitly.
    """
    doc = context.fetch_html(url, params={"lang": "en"}, cache_days=7)
    data: dict[str, str] = {}
    for row in h.xpath_elements(doc, ".//tr[td/strong]"):
        cells = h.xpath_elements(row, "./td")
        if len(cells) != 2:
            continue
        label = h.element_text(cells[0])
        data[label] = h.element_text(cells[1])
    return data


def parse_term(value: str) -> tuple[str | None, str | None]:
    """Split a "DD.MM.YYYY - DD.MM.YYYY" term string into (start, end)."""
    parts = value.split("-")
    if len(parts) != 2:
        return None, None
    return parts[0].strip(), parts[1].strip()


def make_member(context: Context, senator_id: str, name: str, url: str) -> Entity:
    """Build the Person for a senator (id, name, source, citizenship)."""
    person = context.make("Person")
    person.id = context.make_slug(senator_id)
    h.apply_reviewed_name_string(
        context, person, string=name, llm_cleaning=True, lang="msa"
    )
    person.add("name", name)
    person.add("sourceUrl", url)
    # Senate membership requires Malaysian citizenship under Article 47(a) of the
    # Federal Constitution ("Every citizen ... is qualified to be a member of the
    # Senate, if he is not less than thirty years old"; presiding officers are
    # likewise Malaysian office holders):
    # https://lom.agc.gov.my/ (Laws of Malaysia — Federal Constitution)
    person.add("citizenship", "my")
    return person


def emit_occupancy(
    context: Context,
    person: Entity,
    position: Entity,
    categorisation: PositionCategorisation | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Emit an occupancy of `position` held by `person`, if it qualifies as PEP.

    `categorisation` is reused when the caller already categorised the position
    (the shared Member seat, categorised once in `crawl`); otherwise the position
    is categorised here. The position and person are emitted alongside each
    qualifying occupancy, so a person is materialised only if they hold at least
    one PEP position."""
    if categorisation is None:
        categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    context.emit(position)
    context.emit(occupancy)
    context.emit(person)


def emit_officer(context: Context, person: Entity, role: str | None) -> None:
    """Emit the presiding-officer occupancy for a role label, if known.

    Roles absent from `OFFICER_POSITIONS` (ordinary senators and the cabinet titles
    that pollute the role field) are ignored."""
    spec = OFFICER_POSITIONS.get(role) if role is not None else None
    if spec is None:
        return
    name, topics, wikidata_id = spec
    position = h.make_position(
        context,
        name=name,
        country="my",
        topics=topics,
        wikidata_id=wikidata_id,
        lang="eng",
    )
    emit_occupancy(context, person, position)


def emit_cabinet(context: Context, person: Entity, cabinet: str | None) -> None:
    """Emit an executive (cabinet) office held alongside a seat, if any.

    Despite `lang=en`, the cabinet field is inconsistently English or Malay, so
    it is translated (English input passes through unchanged); the position id
    stays keyed on the untranslated name. "&" is normalised to "dan" so the two
    spellings of a ministry collapse to one position."""
    if not cabinet or cabinet == "-":
        return
    position = h.make_position(
        context,
        name=cabinet.replace("&", "dan"),
        country="my",
        lang="msa",
        translate_name=True,
    )
    emit_occupancy(context, person, position)


def crawl_senator(
    context: Context,
    senator_id: str,
    url: str,
    member_position: Entity,
    member_categorisation: PositionCategorisation,
) -> None:
    data = parse_detail(context, url)
    name = data.pop("Name")
    role = data.pop("Position in the Parliament", None)
    cabinet = data.pop("Position in Cabinet", None)
    term = data.pop("Term of Office", None)
    reappointment = data.pop("Reappointment", None)

    person = make_member(context, senator_id, name, url)
    party = data.pop("Party", None)
    if party != "BEBAS":  # BEBAS marks an independent, not a party affiliation
        person.add("political", party)
    person.add("email", h.multi_split(data.pop("Email", ""), ["/", ",", ";"]))

    # Every listed person holds the shared Member seat. The senate term (and any
    # reappointment for a second term) give the occupancy dates.
    for period in (term, reappointment):
        if not period or period == "-":
            continue
        start_date, end_date = parse_term(period)
        if start_date is None or end_date is None:
            context.log.warning("Unparseable senate term", url=url, term=period)
            continue
        emit_occupancy(
            context,
            person,
            member_position,
            categorisation=member_categorisation,
            start_date=start_date,
            end_date=end_date,
        )

    emit_officer(context, person, role)
    emit_cabinet(context, person, cabinet)

    context.audit_data(
        data,
        ignore=[
            "Appointment",
            "State",
            "Phone Number",
            "Fax No.",
            "Social Media",
            "Mailing Address",
        ],
    )


def iter_member_links(
    context: Context, roster_url: str, minimum: int
) -> Iterator[tuple[str, str]]:
    """Yield (senator_id, profile_url) for each senator listed on the chamber roster."""
    doc = context.fetch_html(roster_url, absolute_links=True, cache_days=1)
    links = h.xpath_strings(
        doc,
        ".//ul[contains(@class,'member-of-parliament')]/li//a[contains(@href,'id=')]/@href",
    )
    if len(links) < minimum:
        raise ValueError(
            "Unexpectedly few senators at %s: %d" % (roster_url, len(links))
        )
    for link in links:
        match = re.search(r"[?&]id=(\d+)", link)
        if match is None:
            raise ValueError("Senator link without id: %s" % link)
        yield match.group(1), link


def crawl(context: Context) -> None:
    context.http.verify = False

    # The single Member seat is held by all senators, so it is built and
    # categorised once here and passed down; the exceptional roles (presiding
    # officers, cabinet offices) are categorised as encountered.
    member_position = h.make_position(
        context,
        name="Member of the Dewan Negara",
        country="my",
        topics=TOPICS,
        wikidata_id="Q21328606",
        lang="eng",
    )
    member_categorisation = categorise(context, member_position)
    if member_categorisation.is_pep:
        context.emit(member_position)

    for senator_id, url in iter_member_links(
        context, context.data_url, DEWAN_NEGARA_MIN
    ):
        crawl_senator(context, senator_id, url, member_position, member_categorisation)

    assert_all_accepted(context, raise_on_unaccepted=False)
