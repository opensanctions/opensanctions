"""
Crawler for extracting names and constituencies of Hong Kong
Legislative Council members.
"""

from typing import Any, Dict, NamedTuple, Optional, Union

from zavod import Context, Entity
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
) -> Optional[Entity]:
    """Fetch personal information for a Legislative Council member and
    create a Person entity."""
    person = context.make("Person")
    person.id = context.make_id(
        str(member.pop("member_id")), str(member.pop("search_key"))
    )
    context.log.debug("Unique ID {person_id}".format(person_id=person.id))
    # Add names in English and both Chinese writing systems
    h.apply_name(person, full=pages.en.pop("name"), lang="en")
    h.apply_name(person, full=pages.hant.pop("name"), lang="zh_hant")
    h.apply_name(person, full=pages.hans.pop("name"), lang="zh_hans")
    for email in pages.en.pop("email_address", []):
        if email:
            context.log.debug("Email: {email}".format(email=email))
            person.add("email", email)
    for url in pages.en.pop("homepage", []):
        if url:
            context.log.debug("Web: {url}".format(url=url))
            person.add("website", url)
    for phone in ("office_telephone", "mobile_phone"):
        for number in pages.en.pop(phone, []):
            if number:
                context.log.debug("Phone: {number}".format(number=number))
                person.add("phone", number)
    title = pages.en.pop("title")
    if title and title != "-":
        context.log.debug("Title: {title}".format(title=title))
        person.add("title", title)
    for qual in pages.en.pop("qualification", []):
        if qual:
            context.log.debug("Education: {qual}".format(qual=qual))
            person.add("education", qual)
    for address in pages.en.pop("office_address", []):
        if address:
            context.log.debug("Address: {address}".format(address=address))
            address_entity = h.make_address(context, address)
            h.apply_address(context, person, address_entity)
    return person


UNUSED_LIST_FIELDS = [
    "seq_num",
    "constituency",
    "constituency_type",
    "photo_url",
    "new_term",
    "enable_link",
    "honour",
]
UNUSED_PAGE_FIELDS = [
    "constituency",
    "constituency_type",
    "photo_url",
    "large_photo_url",
    "office_fax",
    "new_term",
    "enable_link",
    "salute_name",
    "honour",
    "occupation",
    "party",
    "office_address",
]
PAGE_FIELDS = [
    "qualification",
    "title",
    "office_telephone",
    "mobile_phone",
    "email_address",
    "homepage",
]


def crawl_member(
    context: Context,
    member: Dict[str, Union[str, int]],
):
    """Emit entities for a member of the Legislative Council from JSON data."""
    salute_name = member.pop("salute_name")
    context.log.debug("Adding {name}".format(name=salute_name))
    member_id = int(member["member_id"])
    pages = crawl_member_pages(context, member_id)
    person = crawl_person(context, member, pages)
    if person is None:
        return

    positions = []
    if member.pop("is_president", "N") == "Y":
        position = h.make_position(
            context,
            name="President of the Legislative Council of Hong Kong",
            country="Hong Kong",
            topics=["gov.national", "gov.legislative"],
        )
        categorisation = categorise(context, position)
        if categorisation.is_pep:
            positions.append(position)
    position = h.make_position(
        context,
        name="Member of the Legislative Council of Hong Kong",
        country="Hong Kong",
        topics=["gov.national", "gov.legislative"],
    )
    categorisation = categorise(context, position)
    if categorisation.is_pep:
        positions.append(position)
    if not positions:
        return
    for lang, page in zip(pages._fields, pages):
        context.log.debug("Auditing data for {lang}".format(lang=lang))
        assert int(page.pop("member_id")) == member_id
        if lang == "en":
            context.audit_data(page, ignore=UNUSED_PAGE_FIELDS)
        else:
            context.audit_data(page, ignore=UNUSED_PAGE_FIELDS + PAGE_FIELDS)
    context.audit_data(member, ignore=UNUSED_LIST_FIELDS)

    context.emit(person, target=True)
    context.emit(position)
    occupancy = h.make_occupancy(
        context, person, position, True, categorisation=categorisation
    )
    if occupancy is not None:
        context.emit(occupancy)


def crawl(context: Context):
    """Retrieve member list and member information from the JSON API
    and emit entities for council members."""
    assert context.dataset.data is not None
    assert context.dataset.data.url is not None
    response = context.fetch_json(context.dataset.data.url, cache_days=1)
    members = response["data"]
    for member in members:
        if member.pop("is_active", "N") == "N":
            continue
        crawl_member(context, member)
