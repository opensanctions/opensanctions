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
    clean_name = h.strip_name_titles(context, raw_name)
    assert clean_name is not None
    party_type = member.pop("partyType")
    if party_type not in (CONSTITUENCY, PARTY_LIST):
        context.log.warning("Unknown party type", value=party_type, member=member_id)

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
    # Constituency members represent a numbered district within a province; party-list
    # members have no district.
    province = member.pop("province", None)
    district = member.pop("district", None)
    if party_type == CONSTITUENCY and province and district:
        occupancy.add("constituency", f"{province} {district}", lang="tha")
    elif province:
        occupancy.add("constituency", province, lang="tha")

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

    # Fetch pages until the API returns an empty batch; the expected roster size is
    # enforced by the dataset assertions.
    for page in count(1):
        data = context.fetch_json(
            context.data_url,
            method="POST",
            params={"page": page},
            cache_days=14,
        )
        members = data["arrdata"]
        if not members:
            break
        for member in members:
            crawl_member(context, position, categorisation, member)
