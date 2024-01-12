"""
Crawler for extracting names and constituencies of Hong Kong
Legislative Council members.
"""

from typing import Dict, Union, List

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

MEMBER_URL_FORMAT = (
    "https://app4.legco.gov.hk/mapi/{lang}/api/LASS/getMember?member_id={member_id}"
)


def crawl_member(
    context: Context,
    member: Dict[str, Union[str, int]],
    pages: Dict[str, Dict[str, Union[str, int, List]]],
):
    context.log.info(
        "Adding {name} ({name_tc})".format(
            name=pages["en"]["name"], name_tc=pages["tc"]["name"]
        )
    )
    person = context.make("Person")
    person.id = context.make_id(member["member_id"])
    h.apply_name(person, full=pages["en"]["name"])
    if member.get("is_president", "N") == "Y":
        position_name = "President of the Legislative Council"
    else:
        position_name = "Member of the Legislative Council"
    position = h.make_position(
        context,
        name=position_name,
        country="China",
        subnational_area="Hong Kong Special Administrative Region",
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


def crawl(context: Context):
    """Retrieve member list and member information from the JSON API
    and emit entities for council members."""
    response = context.fetch_json(context.dataset.data.url, cache_days=7)
    members = response["data"]
    for member in members:
        if member.get("is_active", "N") == "N":
            continue
        member_id = member["member_id"]
        # Retrieve member pages in English, traditional and simplified Chinese
        pages = {}
        for lang in "en", "tc", "cn":
            member_url = MEMBER_URL_FORMAT.format(lang=lang, member_id=member_id)
            response = context.fetch_json(member_url, cache_days=7)
            pages[lang] = response["data"]
        crawl_member(context, member, pages)
