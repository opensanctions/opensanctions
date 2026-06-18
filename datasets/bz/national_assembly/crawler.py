from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from lxml.etree import _Element


# Party names that may appear in the role/affiliation line.
PARTIES = ["People's United Party", "United Democratic Party"]


def parse_members(doc: _Element) -> list[tuple[str, str]]:
    """Pair each present member's name (h4 title) with its role/affiliation line (h5).

    Both pages render one title-heading and one custom-heading per sitting member, in
    document order, so they pair positionally. Past office-holders are listed elsewhere
    (not as title headings), so this only yields current members."""
    names = [
        h.element_text(e)
        for e in h.xpath_elements(doc, ".//h4[contains(@class, 'title-heading')]")
    ]
    roles = [
        h.element_text(e)
        for e in h.xpath_elements(doc, ".//h5[contains(@class, 'vc_custom_heading')]")
    ]
    if len(names) != len(roles):
        raise ValueError(
            "Member name/role count mismatch: %d names, %d roles"
            % (len(names), len(roles))
        )
    return list(zip(names, roles))


def emit_member(
    context: Context,
    name: str,
    role: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    name = name.replace("Hon.", "").strip()

    person = context.make("Person")
    person.id = context.make_id(position.id, name)
    person.add("name", name)
    # Members of both chambers must be citizens of Belize (Constitution of Belize, s. 57
    # for the House and s. 62 for the Senate); the Commonwealth-citizen allowance applies
    # only to voting, not to membership.
    # https://www.nationalassembly.gov.bz/wp-content/uploads/2022/01/Belize-Constitution-Chapter-4-2021.pdf
    person.add("citizenship", "bz")
    matched = [party for party in PARTIES if party in role]
    if matched:
        person.add("political", matched)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def make_chamber_position(
    context: Context, name: str, wikidata_id: str
) -> tuple[Entity, PositionCategorisation]:
    position = h.make_position(
        context,
        name=name,
        country="bz",
        topics=["gov.national", "gov.legislative"],
        wikidata_id=wikidata_id,
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)
    return position, categorisation


def crawl_representatives(context: Context) -> None:
    # House of Representatives: the Speaker plus the elected members.
    house_doc = context.fetch_html(
        context.data_url + "house-of-representatives/", cache_days=1
    )
    members = parse_members(house_doc)
    speaker_pos, speaker_cat = make_chamber_position(
        context, "Speaker of the House of Representatives of Belize", "Q6597925"
    )
    rep_pos, rep_cat = make_chamber_position(
        context, "Member of the House of Representatives of Belize", "Q21290854"
    )
    for name, role in members:
        if "Speaker" in role:
            emit_member(context, name, role, speaker_pos, speaker_cat)
        else:
            emit_member(context, name, role, rep_pos, rep_cat)


def crawl_senators(context: Context) -> None:
    # Senate: the President plus the appointed senators.
    senate_doc = context.fetch_html(context.data_url + "senate/", cache_days=1)
    senators = parse_members(senate_doc)
    if len(senators) < 10:
        raise ValueError("Expected at least 10 senators, found %d" % len(senators))
    pres_pos, pres_cat = make_chamber_position(
        context, "President of the Senate of Belize", "Q6594868"
    )
    sen_pos, sen_cat = make_chamber_position(
        context, "Member of the Senate of Belize", "Q21295124"
    )
    for name, role in senators:
        if "President of the Senate" in role:
            emit_member(context, name, role, pres_pos, pres_cat)
        else:
            emit_member(context, name, role, sen_pos, sen_cat)


def crawl(context: Context) -> None:
    crawl_representatives(context)
    crawl_senators(context)
