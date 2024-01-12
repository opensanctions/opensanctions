"""
Crawler for extracting names and constituencies of Hong Kong
Legislative Council members.
"""

from typing import Dict, Union, NamedTuple

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


class SearchKeyData(NamedTuple):
    """Components of "search_key" field in member list."""

    name_zh_hant: str  # Name (traditional)
    name_zh_hans: str  # Name (simplified)
    name_en: str  # Name (English)
    cons_name_hant: str  # Constituency name (traditional)
    cons_name_hans: str  # Constituency name (simplified)
    cons_name_en: str  # Constituency name (English)
    cons_type_hant: str  # Constituency type (traditional)
    cons_type_hans: str  # Constituency type (simplified)
    cons_type_en: str  # Constituency type (English)


def extract_search_key(key: str) -> SearchKeyData:
    """Extract fields from search_key."""
    return SearchKeyData(*key.split("|"))


def crawl_member(
    context: Context,
    member: Dict[str, Union[str, int]],
):
    """Emit entities for a member of the Legislative Council from JSON data."""
    context.log.info("Adding {name}".format(name=member["salute_name"]))
    keydata = extract_search_key(member["search_key"])
    context.log.info("Chinese name: {name}".format(name=keydata.name_zh_hant))
    person = context.make("Person")
    person.id = context.make_id(member["member_id"], member["search_key"])
    h.apply_name(person, full=member["salute_name"])
    h.apply_name(person, full=keydata.name_zh_hant, lang="zh_hant")
    h.apply_name(person, full=keydata.name_zh_hans, lang="zh_hans")
    if member.get("is_president", "N") == "Y":
        position_name = "President of the Legislative Council"
    else:
        position_name = "Member of the Legislative Council"
    position = h.make_position(
        context,
        name=position_name,
        country="Hong Kong",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context, person, position, True, categorisation=categorisation
    )
    context.emit(person, target=True)
    context.emit(position)
    context.emit(occupancy)
    # Note: There is additional information available on the member
    # page, at
    # https://app4.legco.gov.hk/mapi/{lang}/api/LASS/getMember?member_id={member_id}
    # though it doesn't quite fit the schema.


def crawl(context: Context):
    """Retrieve member list and member information from the JSON API
    and emit entities for council members."""
    response = context.fetch_json(context.dataset.data.url, cache_days=7)
    members = response["data"]
    for member in members:
        if member.get("is_active", "N") == "N":
            continue
        crawl_member(context, member)
