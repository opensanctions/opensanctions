"""
Crawler for extracting names and constituencies of Hong Kong
Legislative Council members.
"""

from typing import Any, NamedTuple

from banal import ensure_list
from zavod import Context, Entity
from zavod import helpers as h

MEMBER_URL_FORMAT = (
    "https://app4.legco.gov.hk/mapi/{lang}/api/LASS/getMember?member_id={member_id}"
)


class MemberPages(NamedTuple):
    """Member pages in English, Traditional and Simplified Chinese"""

    en: dict[str, Any]
    hant: dict[str, Any]
    hans: dict[str, Any]


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
    member: dict[str, str | int],
    pages: MemberPages,
) -> Entity:
    """Fetch personal information for a Legislative Council member and
    create a Person entity."""
    person = context.make("Person")
    person.id = context.make_id(
        str(member.pop("member_id")), str(member.pop("search_key"))
    )
    # citizenship not required (Art 67): https://www.cmab.gov.hk/doc/en/documents/policy_responsibilities/Racial_Discrimination/AnnexI-Eng.pdf
    person.add("country", "hk")
    context.log.debug(f"Unique ID {person.id}")
    # Add names in English and both Chinese writing systems
    h.apply_name(person, full=pages.en.pop("name"), lang="eng")
    h.apply_name(person, full=pages.hant.pop("name"), lang="zho")
    h.apply_name(person, full=pages.hans.pop("name"), lang="zho")
    for email in ensure_list(pages.en.pop("email_address", [])):
        context.log.debug(f"Email: {email}")
        person.add("email", email)
    for url in ensure_list(pages.en.pop("homepage", [])):
        if url is not None:
            context.log.debug(f"Web: {url}")
            person.add("website", url)
    for phone in ("office_telephone", "mobile_phone"):
        for number in ensure_list(pages.en.pop(phone, [])):
            if number:
                context.log.debug(f"Phone: {number}")
                person.add("phone", number)
    title = pages.en.pop("title")
    if title and title != "-":
        person.add("title", title)
    for qual in ensure_list(pages.en.pop("qualification", [])):
        if qual:
            context.log.debug(f"Education: {qual}")
            person.add("education", qual)
    for address in ensure_list(pages.en.pop("office_address", [])):
        if address:
            context.log.debug(f"Address: {address}")
            address_entity = h.make_address(context, address)
            h.copy_address(person, address_entity)
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
    "constituency_type",
    "photo_url",
    "large_photo_url",
    "office_fax",
    "new_term",
    "enable_link",
    "honour",
    "office_address",
    "office_list",
    "annual_report_list",
    "other_report_list",
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
    member: dict[str, str | int],
) -> None:
    """Emit entities for a member of the Legislative Council from JSON data."""
    salute_name = member.pop("salute_name")
    context.log.debug(f"Adding {salute_name}")
    member_id = int(member["member_id"])
    pages = crawl_member_pages(context, member_id)
    person = crawl_person(context, member, pages)

    position = h.make_position(
        context,
        name="Member of the Legislative Council of Hong Kong",
        country="hk",
        topics=["gov.national", "gov.legislative"],
    )
    if member.pop("is_president", "N") == "Y":
        position = h.make_position(
            context,
            name="President of the Legislative Council of Hong Kong",
            country="hk",
            topics=["gov.national", "gov.legislative"],
        )

    occupancy = h.make_occupancy(
        context,
        person,
        position,
    )
    if occupancy is None:
        return

    for lang, page in zip(pages._fields, pages):
        context.log.debug(f"Auditing data for {lang}")
        assert int(page.pop("member_id")) == member_id
        if lang in ("hant", "hans"):
            lang = "zho"
        person.add("political", page.pop("party", None), lang=lang)
        person.add("alias", page.pop("salute_name", None), lang=lang)
        person.add("profession", page.pop("occupation", None), lang=lang)
        occupancy.add("constituency", page.pop("constituency", None), lang=lang)

        if lang == "en":
            context.audit_data(page, ignore=UNUSED_PAGE_FIELDS)
        else:
            context.audit_data(page, ignore=UNUSED_PAGE_FIELDS + PAGE_FIELDS)

    context.emit(person)
    context.emit(position)
    context.emit(occupancy)
    context.audit_data(member, ignore=UNUSED_LIST_FIELDS)


def crawl(context: Context) -> None:
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
