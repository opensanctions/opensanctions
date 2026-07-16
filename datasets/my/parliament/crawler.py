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

# Minimum roster sizes
DEWAN_RAKYAT_MIN = 180
DEWAN_NEGARA_MIN = 40
DEFAULT_PARLIAMENT_TOPICS = ["gov.legislative", "gov.national"]

# field is inconsistently localized: most values arrive in Malay
# even under `lang=en`, so most keys are Malay, but the House clerk arrives in English
OFFICER_POSITIONS: dict[str, tuple[str, list[str], str | None]] = {
    "Yang di-Pertua Dewan Rakyat": (
        "Speaker of the Dewan Rakyat",
        DEFAULT_PARLIAMENT_TOPICS,
        "Q7574262",
    ),
    "Timbalan Yang di-Pertua Dewan Rakyat": (
        "Deputy Speaker of the Dewan Rakyat",
        DEFAULT_PARLIAMENT_TOPICS,
        "Q126361900",
    ),
    "Ketua Majlis": ("Leader of the Dewan Rakyat", DEFAULT_PARLIAMENT_TOPICS, None),
    "Timbalan Ketua Majlis": (
        "Deputy Leader of the Dewan Rakyat",
        DEFAULT_PARLIAMENT_TOPICS,
        None,
    ),
    "Secretary Of The House Of Representatives": (
        "Secretary of the Dewan Rakyat",
        ["gov.admin", "gov.national"],
        None,
    ),
    "Yang di-Pertua Dewan Negara": (
        "President of the Dewan Negara",
        DEFAULT_PARLIAMENT_TOPICS,
        "Q7241319",
    ),
    "Timbalan Yang di-Pertua Dewan Negara": (
        "Deputy President of the Dewan Negara",
        DEFAULT_PARLIAMENT_TOPICS,
        "Q134572656",
    ),
}


def parse_detail(context: Context, url: str) -> dict[str, str]:
    """Return the label -> value pairs of a member's `MAKLUMAT` info table.

    `lang=en` yields English field labels ("Name", "Position in the Parliament",
    ...); the detail links on the roster pages omit it, so it is requested here.
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


def constituency_name(data: dict[str, str]) -> str | None:
    """Build a human-readable constituency from a representative's detail table."""
    parts = [data.get("Area"), data.get("State")]
    labels = [p for p in parts if p and p != "-"]
    if not labels:
        return None
    return ", ".join(labels)


def make_member(context: Context, member_id: str, name: str, url: str) -> Entity:
    """Build the Person shared by both houses (id, name, source, citizenship)."""
    person = context.make("Person")
    person.id = context.make_slug(member_id)
    h.apply_reviewed_name_string(
        context, person, string=name, llm_cleaning=True, lang="msa"
    )
    person.add("sourceUrl", url)
    # Membership of either house of Parliament requires Malaysian citizenship
    # under Article 47 of the Federal Constitution of Malaysia (presiding officers
    # are likewise Malaysian office holders):
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
    constituency: str | None = None,
) -> None:
    """Emit an occupancy of `position` held by `person`, if it qualifies as PEP.

    `categorisation` is reused when the caller already categorised the position
    (the shared Member seats, categorised once in `crawl`); otherwise the position
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
    occupancy.add("constituency", constituency)
    context.emit(position)
    context.emit(occupancy)
    context.emit(person)


def emit_officer(context: Context, person: Entity, role: str | None) -> None:
    """Emit the presiding-officer occupancy for a role label, if known.

    Roles absent from `OFFICER_POSITIONS` (ordinary members and the cabinet titles
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

    Cabinet titles are Malay (and, for the Senate, sometimes already English), so
    they are translated to English; the position id stays keyed on the
    untranslated name. "&" is normalised to "dan" so the two spellings of a
    ministry collapse to one position."""
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


def crawl_representative(
    context: Context,
    member_id: str,
    url: str,
    member_position: Entity,
    member_categorisation: PositionCategorisation,
) -> None:
    data = parse_detail(context, url)

    person = make_member(context, member_id, data.pop("Name"), url)
    party = data.pop("Party", None)
    if party != "BEBAS":  # BEBAS marks an independent, not a party affiliation
        person.add("political", party)
    person.add("email", h.multi_split(data.pop("Email", ""), ["/", ",", ";"]))

    # `Parliament` is the constituency code; its presence marks an elected MP.
    # A member with a constituency (P-code) is an elected MP holding the shared
    # Member seat, with the constituency recorded on the occupancy.
    role = data.pop("Position in the Parliament", None)
    if data.pop("Parliament"):
        emit_occupancy(
            context,
            person,
            member_position,
            categorisation=member_categorisation,
            constituency=constituency_name(data),
        )
        # A sitting MP may additionally be the Deputy Speaker.
        emit_officer(context, person, role)
    elif role in OFFICER_POSITIONS:
        # The Speaker and the Secretary (House clerk) hold no constituency, identified by role.
        emit_officer(context, person, role)
    else:
        context.log.warning(
            "Member without constituency or known role", url=url, role=role
        )
        return
    emit_cabinet(context, person, data.pop("Position in Cabinet", None))

    context.audit_data(
        data,
        ignore=[
            "Seat Number",
            "Phone Number",
            "Fax No.",
            "Social Media",
            "Mailing Address",
            "Area",
            "State",
        ],
    )


def crawl_senator(
    context: Context,
    senator_id: str,
    url: str,
    member_position: Entity,
    member_categorisation: PositionCategorisation,
) -> None:
    data = parse_detail(context, url)

    person = make_member(context, senator_id, data.pop("Name"), url)
    party = data.pop("Party", None)
    if party != "BEBAS":  # BEBAS marks an independent, not a party affiliation
        person.add("political", party)
    person.add("email", h.multi_split(data.pop("Email", ""), ["/", ",", ";"]))

    # Every listed person holds the shared Member seat. The senate term (and any
    # reappointment for a second term) give the occupancy dates.
    for period in (data.pop("Term of Office", None), data.pop("Reappointment", None)):
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

    emit_officer(context, person, data.pop("Position in the Parliament", None))
    emit_cabinet(context, person, data.pop("Position in Cabinet", None))

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
    """Yield (member_id, profile_url) for each member listed on a chamber roster."""
    doc = context.fetch_html(roster_url, absolute_links=True, cache_days=1)
    links = h.xpath_strings(
        doc,
        ".//ul[contains(@class,'member-of-parliament')]/li//a[contains(@href,'id=')]/@href",
    )
    if len(links) < minimum:
        raise ValueError(
            "Unexpectedly few members at %s: %d" % (roster_url, len(links))
        )
    for link in links:
        match = re.search(r"[?&]id=(\d+)", link)
        if match is None:
            raise ValueError("Member link without id: %s" % link)
        yield match.group(1), link


def crawl_rakyat(context: Context) -> None:
    rakyat_position = h.make_position(
        context,
        name="Member of the Dewan Rakyat",
        country="my",
        topics=DEFAULT_PARLIAMENT_TOPICS,
        wikidata_id="Q21290861",
        lang="eng",
    )
    rakyat_categorisation = categorise(context, rakyat_position)
    if rakyat_categorisation.is_pep:
        context.emit(rakyat_position)

    for member_id, url in iter_member_links(
        context, context.data_url + "ahli-dewan.html?uweb=dr&lang=en", DEWAN_RAKYAT_MIN
    ):
        crawl_representative(
            context, member_id, url, rakyat_position, rakyat_categorisation
        )


def crawl_negara(context: Context) -> None:
    negara_position = h.make_position(
        context,
        name="Member of the Dewan Negara",
        country="my",
        topics=DEFAULT_PARLIAMENT_TOPICS,
        wikidata_id="Q21328606",
        lang="eng",
    )
    negara_categorisation = categorise(context, negara_position)
    if negara_categorisation.is_pep:
        context.emit(negara_position)

    for member_id, url in iter_member_links(
        context,
        context.data_url + "ahli-dewan-dn.html?uweb=dn&lang=en",
        DEWAN_NEGARA_MIN,
    ):
        crawl_senator(context, member_id, url, negara_position, negara_categorisation)


def crawl(context: Context) -> None:
    crawl_rakyat(context)
    crawl_negara(context)
    assert_all_accepted(context, raise_on_unaccepted=False)
