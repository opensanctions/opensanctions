from itertools import count
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import LangText

# partyType values: single-member constituency vs national party list.
CONSTITUENCY = "แบบแบ่งเขต"
PARTY_LIST = "แบบบัญชีรายชื่อ"


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member: dict[str, Any],
) -> None:
    member_id = member.pop("ID_member")
    raw_name = member.pop("name")
    # The roster spans all 500 seats. A seat awaiting a by-election or a party-list
    # replacement is published with its party set but no holder name, so skip it
    # rather than emitting a nameless Person. The dataset's min-Person assertion
    # catches a source regression that blanks names wholesale.
    if not raw_name.strip():
        context.log.info("Seat has no named holder", member=member_id)
        return
    clean_name = h.strip_name_titles(context, raw_name)
    if clean_name is None:
        # strip_name_titles has already warned that the name was only titles.
        return

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    original_name = raw_name if clean_name != raw_name else None
    person.add("name", clean_name, lang="tha", original_value=original_name)
    apply_translit_full_name(context, person, LangText(clean_name, "tha"))
    person.add("political", member.pop("category_party"), lang="tha")
    # A candidate for the House of Representatives must be of Thai nationality by birth
    # (Constitution of Thailand 2017, Section 97(1)).
    # https://www.constituteproject.org/constitution/Thailand_2017
    person.add("citizenship", "th")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return

    party_type = member.pop("partyType")
    province = member.pop("province")
    district = member.pop("district")
    if party_type == CONSTITUENCY:
        # Constituency members represent a numbered district within a province.
        assert province and district, (member_id, province, district)
        occupancy.add("constituency", f"{province} {district}", lang="tha")
    elif party_type == PARTY_LIST:
        # Party-list members are elected nationally, so they have no district.
        assert not province and district is None, (member_id, province, district)
    else:
        # Don't guess at a constituency for a member whose route into the House we
        # don't recognise. The rest of their record is still valid.
        context.log.warning(
            "Unknown party type", party_type=party_type, member=member_id
        )

    # category is an internal classification code, NO_member a seat number, and
    # profileImage a photo URL, which Person:images cannot hold (it links entities).
    context.audit_data(member, ignore=["category", "NO_member", "profileImage"])
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Representatives of Thailand",
        country="th",
        wikidata_id="Q21290865",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Each response reports the full roster size, so page until we've seen that many
    # members rather than trusting an empty batch to mean the roster is exhausted.
    seen = 0
    for page in count(1):
        data = context.fetch_json(
            context.data_url, method="POST", params={"page": page}
        )
        total = data["total"]
        members = data["arrdata"]
        # A page running dry before the roster is complete would silently under-collect.
        assert len(members) > 0, (page, seen, total)
        for member in members:
            crawl_member(context, position, categorisation, member)
        seen += len(members)
        if seen >= total:
            break
    assert seen == total, (seen, total)
