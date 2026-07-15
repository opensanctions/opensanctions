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
DEWAN_RAKYAT_MIN = 180
TOPICS = ["gov.legislative", "gov.national"]

# The `Position in the Parliament` field is inconsistently localized: most values
# arrive in Malay even under `lang=en`, but the House clerk arrives in English.
OFFICER_POSITIONS: dict[str, tuple[str, list[str], str | None]] = {
    "Yang di-Pertua Dewan Rakyat": (
        "Speaker of the Dewan Rakyat",
        TOPICS,
        "Q7574262",
    ),
    "Timbalan Yang di-Pertua Dewan Rakyat": (
        "Deputy Speaker of the Dewan Rakyat",
        TOPICS,
        "Q126361900",
    ),
    "Ketua Majlis": ("Leader of the Dewan Rakyat", TOPICS, None),
    "Timbalan Ketua Majlis": ("Deputy Leader of the Dewan Rakyat", TOPICS, None),
    "Secretary Of The House Of Representatives": (
        "Secretary of the Dewan Rakyat",
        ["gov.admin", "gov.national"],
        None,
    ),
}


def parse_detail(context: Context, url: str) -> dict[str, str]:
    """Return the label -> value pairs of a member's `MAKLUMAT` info table.

    `lang=en` yields English field labels ("Name", "Position in the Parliament",
    ...); the detail links on the roster page omit it, so it is requested here.
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


def constituency_name(data: dict[str, str]) -> str | None:
    """Build a human-readable constituency from a representative's detail table."""
    parts = [data.get("Area"), data.get("State")]
    labels = [p for p in parts if p and p != "-"]
    if not labels:
        return None
    return ", ".join(labels)


def make_member(context: Context, member_id: str, name: str, url: str) -> Entity:
    """Build the Person for a representative (id, name, source, citizenship)."""
    person = context.make("Person")
    person.id = context.make_slug(member_id)
    h.apply_reviewed_name_string(
        context, person, string=name, llm_cleaning=True, lang="msa"
    )
    person.add("name", name)
    person.add("sourceUrl", url)
    # Membership of the House of Representatives requires Malaysian citizenship
    # under Article 47 of the Federal Constitution of Malaysia (the presiding
    # officers are likewise Malaysian office holders):
    # https://lom.agc.gov.my/ (Laws of Malaysia — Federal Constitution)
    person.add("citizenship", "my")
    return person


def emit_occupancy(
    context: Context,
    person: Entity,
    position: Entity,
    categorisation: PositionCategorisation | None = None,
    constituency: str | None = None,
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

    Cabinet titles are Malay, so they are translated to English; the position id
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


def crawl_member(
    context: Context,
    member_id: str,
    url: str,
    member_position: Entity,
    member_categorisation: PositionCategorisation,
) -> None:
    data = parse_detail(context, url)

    name = data.pop("Name")
    role = data.pop("Position in the Parliament", None)
    cabinet = data.pop("Position in Cabinet", None)

    # `Parliament` is the constituency code (e.g. "P075"); its presence marks an
    # elected MP. The readable constituency name is built from `Area`/`State`.
    constituency_code = data.pop("Parliament")
    constituency = constituency_name(data)

    person = make_member(context, member_id, name, url)
    party = data.pop("Party", None)
    if party != "BEBAS":  # BEBAS marks an independent, not a party affiliation
        person.add("political", party)
    person.add("email", h.multi_split(data.pop("Email", ""), ["/", ",", ";"]))

    # A member with a constituency (P-code) is an elected MP holding the shared
    # Member seat, with the constituency recorded on the occupancy. The presiding
    # officers (Speaker, Secretary) hold no constituency and are identified by role.
    if constituency_code:
        emit_occupancy(
            context,
            person,
            member_position,
            categorisation=member_categorisation,
            constituency=constituency,
        )
        # A sitting MP may additionally be the Deputy Speaker.
        emit_officer(context, person, role)
    elif role in OFFICER_POSITIONS:
        # The Speaker and the Secretary (House clerk) hold no constituency.
        emit_officer(context, person, role)
    else:
        context.log.warning(
            "Member without constituency or known role", url=url, role=role
        )
        return

    emit_cabinet(context, person, cabinet)

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


def iter_member_links(
    context: Context, roster_url: str, minimum: int
) -> Iterator[tuple[str, str]]:
    """Yield (member_id, profile_url) for each member listed on the chamber roster."""
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


def crawl(context: Context) -> None:
    context.http.verify = False

    # The single Member seat is held by all ordinary members, so it is built and
    # categorised once here and passed down; the exceptional roles (presiding
    # officers, cabinet offices) are categorised as encountered.
    member_position = h.make_position(
        context,
        name="Member of the Dewan Rakyat",
        country="my",
        topics=TOPICS,
        wikidata_id="Q21290861",
        lang="eng",
    )
    member_categorisation = categorise(context, member_position)
    if member_categorisation.is_pep:
        context.emit(member_position)

    for member_id, url in iter_member_links(
        context, context.data_url, DEWAN_RAKYAT_MIN
    ):
        crawl_member(context, member_id, url, member_position, member_categorisation)

    assert_all_accepted(context, raise_on_unaccepted=False)
