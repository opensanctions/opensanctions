"""
Crawler for extracting names and constituencies of Hong Kong
Legislative Council members.
"""

from typing import Dict, Union, NamedTuple, Any

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise


MEMBER_URL_FORMAT = (
    "https://app4.legco.gov.hk/mapi/{lang}/api/LASS/getMember?member_id={member_id}"
)


class MemberPages(NamedTuple):
    """Member pages in English, Traditional and Simplified Chinese"""

    en: Dict[str, Any]
    hant: Dict[str, Any]
    hans: Dict[str, Any]


def crawl_member_pages(context: Context, member_id: int) -> MemberPages:
    # Retrieve member pages in English, traditional and simplified Chinese
    pages = []
    for lang in "en", "tc", "cn":
        member_url = MEMBER_URL_FORMAT.format(lang=lang, member_id=member_id)
        response = context.fetch_json(member_url, cache_days=7)
        pages.append(response["data"])
    return MemberPages(*pages)


def crawl_person(
    context: Context,
    member: Dict[str, Union[str, int]],
    pages: MemberPages,
):
    """Fetch personal information for a Legislative Council member and
    create a Person entity."""
    person = context.make("Person")
    person.id = context.make_id(member["member_id"], member["search_key"])
    context.log.info("Unique ID {person_id}".format(person_id=person.id))
    # Add names in English and both Chinese writing systems
    h.apply_name(person, full=pages.en["name"], lang="en")
    context.log.info("English name {name}".format(name=pages.en["name"]))
    h.apply_name(person, full=pages.hant["name"], lang="zh_hant")
    context.log.info("Traditional Chinese name {name}".format(name=pages.hant["name"]))
    h.apply_name(person, full=pages.hans["name"], lang="zh_hans")
    context.log.info("Simplified Chinese name {name}".format(name=pages.hans["name"]))
    for email in pages.en.get("email_address", []):
        if email:
            context.log.info("Email: {email}".format(email=email))
            person.add("email", email)
    for url in pages.en.get("homepage", []):
        if url:
            context.log.info("Web: {url}".format(url=url))
            person.add("website", url)
    for phone in ("office_telephone", "mobile_phone"):
        for number in pages.en.get(phone, []):
            if number:
                context.log.info("Phone: {number}".format(number=number))
                person.add("phone", number)
    honours = pages.en.get("honour")
    if honours and honours != "-":
        context.log.info("Honours: {honours}".format(honours=honours))
        person.add("title", honours)
    for qual in pages.en.get("qualification", []):
        if qual:
            context.log.info("Education: {qual}".format(qual=qual))
            person.add("education", qual)
    for address in pages.en.get("office_address", []):
        if address:
            context.log.info("Address: {address}".format(address=address))
            address_entity = h.make_address(context, address)
            h.apply_address(context, person, address_entity)
    return person


def crawl_member(
    context: Context,
    member: Dict[str, Union[str, int]],
):
    """Emit entities for a member of the Legislative Council from JSON data."""
    context.log.info("Adding {name}".format(name=member["salute_name"]))
    pages = crawl_member_pages(context, member["member_id"])
    person = crawl_person(context, member, pages)

    positions = []
    if member.get("is_president", "N") == "Y":
        position = h.make_position(
            context,
            name="President of the Legislative Council of Hong Kong",
            country="Hong Kong",
            topics=["gov.national", "gov.legislative", "role.pep"],
        )
        categorisation = categorise(context, position)
        if categorisation.is_pep:
            positions.append(position)
    position = h.make_position(
        context,
        name="Member of the Legislative Council of Hong Kong",
        country="Hong Kong",
        topics=["gov.national", "gov.legislative", "role.pep"],
    )
    categorisation = categorise(context, position)
    if categorisation.is_pep:
        positions.append(position)
    if not positions:
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
        crawl_member(context, member)
