import re
from typing import Iterator

from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.stateful.review import assert_all_accepted

from zavod import Context
from zavod import helpers as h

# Chamber roster pages. `lang=en` yields English field labels on the linked
# detail pages (see `parse_detail`).
RAKYAT_URL = "https://www.parlimen.gov.my/ahli-dewan.html?uweb=dr&lang=en"
NEGARA_URL = "https://www.parlimen.gov.my/ahli-dewan-dn.html?uweb=dn&lang=en"

DEFAULT_PARLIAMENT_TOPICS = ["gov.legislative", "gov.national"]

REPRESENTATIVE_IGNORE = [
    "Seat Number",
    "Phone Number",
    "Fax No.",
    "Social Media",
    "Mailing Address",
    "Area",
    "State",
]
SENATOR_IGNORE = [
    "Appointment",
    "State",
    "Phone Number",
    "Fax No.",
    "Social Media",
    "Mailing Address",
]


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


def apply_member_details(
    context: Context, person: Entity, data: dict[str, str], url: str
) -> None:
    """Populate the fields shared by both houses on an already-created person.

    Consumes the `Name`, `Party` and `Email` entries from the detail table."""
    clean_name = h.strip_name_titles(context, data.pop("Name"))
    h.apply_reviewed_name_string(
        context, person, string=clean_name, llm_cleaning=True, lang="msa"
    )
    person.add("sourceUrl", url)
    # Membership of either house of Parliament requires Malaysian citizenship
    # under Article 47 of the Federal Constitution of Malaysia (presiding officers
    # are likewise Malaysian office holders):
    # https://lom.agc.gov.my/ (Laws of Malaysia — Federal Constitution)
    person.add("citizenship", "my")
    party = data.pop("Party", None)
    if party != "BEBAS":  # BEBAS marks an independent, not a party affiliation
        person.add("political", party)
    person.add("email", h.multi_split(data.pop("Email", ""), ["/", ",", ";"]))


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


def emit_officer(context: Context, person: Entity, url: str, role: str | None) -> bool:
    """Emit a presiding-officer occupancy for a "Position in the Parliament" label.

    Returns True when `role` names a presiding office and its occupancy was
    emitted. The `position` lookup maps each known office to its
    English position and enumerates the non-office labels (ordinary members,
    blanks, and the cabinet titles the Senate leaks into this field, which are
    already captured from "Position in Cabinet"). An unrecognised label is logged
    rather than silently dropped, so a renamed or newly-created office surfaces."""
    result = context.lookup("position", role)
    if result is None:
        context.log.warning("Unrecognised parliament position", role=role, url=url)
        return False
    if result.name is None:  # a known label that does not denote a presiding office
        return False
    position = h.make_position(
        context,
        name=result.name,
        country="my",
        topics=result.topics,
        wikidata_id=result.wikidata_id,
        lang="eng",
    )
    emit_occupancy(context, person, position)
    return True


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

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    apply_member_details(context, person, data, url)

    # A sitting MP may additionally hold a presiding office (e.g. a Deputy
    # Speaker who also represents a constituency); the Speaker and the Secretary
    # (House clerk) hold a presiding office but no constituency.
    is_officer = emit_officer(
        context, person, url, data.pop("Position in the Parliament", None)
    )
    # `Parliament` is the constituency code; its presence marks an elected MP
    # holding the shared Member seat, with the constituency on the occupancy.
    if data.pop("Parliament"):
        area, state = data.get("Area"), data.get("State")
        constituency = ", ".join(p for p in (area, state) if p and p != "-") or None
        emit_occupancy(
            context,
            person,
            member_position,
            categorisation=member_categorisation,
            constituency=constituency,
        )
    elif not is_officer:
        context.log.warning("Member without constituency or office", url=url)
        return
    emit_cabinet(context, person, data.pop("Position in Cabinet", None))

    context.audit_data(data, ignore=REPRESENTATIVE_IGNORE)


def crawl_senator(
    context: Context,
    senator_id: str,
    url: str,
    member_position: Entity,
    member_categorisation: PositionCategorisation,
) -> None:
    data = parse_detail(context, url)

    person = context.make("Person")
    person.id = context.make_slug(senator_id)
    apply_member_details(context, person, data, url)

    # Every listed person holds the shared Member seat. The senate term (and any
    # reappointment for a second term) give the occupancy dates.
    for period in (data.pop("Term of Office", None), data.pop("Reappointment", None)):
        if not period or period == "-":
            continue
        parts = period.split("-")  # "DD.MM.YYYY - DD.MM.YYYY"
        if len(parts) != 2:
            context.log.warning("Unparseable senate term", url=url, term=period)
            continue
        start_date, end_date = parts[0].strip(), parts[1].strip()
        emit_occupancy(
            context,
            person,
            member_position,
            categorisation=member_categorisation,
            start_date=start_date,
            end_date=end_date,
        )

    emit_officer(context, person, url, data.pop("Position in the Parliament", None))
    emit_cabinet(context, person, data.pop("Position in Cabinet", None))

    context.audit_data(data, ignore=SENATOR_IGNORE)


def iter_member_links(context: Context, roster_url: str) -> Iterator[tuple[str, str]]:
    """Yield (member_id, profile_url) for each member listed on a chamber roster.

    A roster that returns too few members is caught by the dataset assertions
    (the per-chamber `entities_with_prop` minimums), not here."""
    doc = context.fetch_html(roster_url, absolute_links=True, cache_days=1)
    links = h.xpath_strings(
        doc,
        ".//ul[contains(@class,'member-of-parliament')]/li//a[contains(@href,'id=')]/@href",
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

    for member_id, url in iter_member_links(context, RAKYAT_URL):
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

    for member_id, url in iter_member_links(context, NEGARA_URL):
        crawl_senator(context, member_id, url, negara_position, negara_categorisation)


def crawl(context: Context) -> None:
    crawl_rakyat(context)
    crawl_negara(context)
    assert_all_accepted(context, raise_on_unaccepted=False)
