from itertools import count
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

REFERER = "https://www.parliament.go.th/view/1/mpconnect/TH-TH"

# Civilian honorifics prefixed (without a space) to the given name. Military, police and
# academic titles are left in place as they are part of how the member is known.
HONORIFICS = ("นางสาว", "นาย", "นาง")

# partyType values: single-member constituency vs national party list.
CONSTITUENCY = "แบบแบ่งเขต"
PARTY_LIST = "แบบบัญชีรายชื่อ"


def clean_name(raw: str) -> str:
    name = " ".join(raw.split())
    for honorific in HONORIFICS:
        if name.startswith(honorific):
            return name[len(honorific) :].strip()
    return name


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member: dict[str, Any],
) -> None:
    member_id = member.pop("ID_member")
    name = clean_name(member.pop("name"))
    assert name, f"Empty name for member {member_id}"
    party_type = member.pop("partyType")
    if party_type not in (CONSTITUENCY, PARTY_LIST):
        context.log.warning("Unknown party type", value=party_type, member=member_id)

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    person.add("name", name, lang="tha")
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
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    seen: set[str] = set()
    total: int | None = None
    for page in count(1):
        data = context.fetch_json(
            context.data_url,
            method="POST",
            params={"page": page},
            headers={"Referer": REFERER},
            cache_days=1,
        )
        if total is None:
            total = data["total"]
        members = data["arrdata"]
        if not members:
            break
        for member in members:
            if member["ID_member"] in seen:
                continue
            seen.add(member["ID_member"])
            crawl_member(context, position, categorisation, member)
        if len(seen) >= total:
            break

    if not seen:
        raise ValueError("No members returned by the HoR endpoint")
