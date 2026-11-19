import json
import re
from typing import Any

from lxml import etree
from normality import squash_spaces
from rigour.mime.types import XML
from zavod.stateful.positions import categorise

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api

IGNORE = [
    # Drupal node bookkeeping.
    "uuid",
    "vid",
    "langcode",
    "type",
    "revision_timestamp",
    "revision_uid",
    "status",
    "uid",
    "created",
    "changed",
    "promote",
    "sticky",
    "default_langcode",
    "revision_translation_affected",
    "moderation_state",
    "metatag",
    "path",
    "content_translation_source",
    "content_translation_outdated",
    "field_meta_tags",
    "field_tags",
    # The agency's own profile and contact details.
    "body",
    "field_applications_shrd",
    "field_email_shrd",
    "field_fax_shrd",
    "field_location_shrd",
    "field_phone_shrd",
    "field_url_shrd",
    "field_na_address",
    "field_na_city",
    "field_na_city_name",
    "field_na_code",
    "field_na_logo",
    "field_na_po_box",
    "field_na_postal_code",
    "field_na_short_name",
    "field_na_social_media",
    "field_na_phone_ext",
    "field_na_phone_contact_time_from",
    "field_na_phone_contact_time_to",
    "field_na_branch_page_link",
    "field_na_branch_opening_time",
    "field_na_branch_closing_time",
    "field_na_branch_wday_start",
    "field_na_branch_wday_end",
    # The agency type, e.g. "Ministries", "Emirates". Ministers are told apart by the
    # agency title instead: the Ministry of Defense is filed under "Other".
    "field_na_type",
    # The head's office contacts and photo; emails and phone numbers are not
    # extracted for PEPs.
    "field_na_officer_email",
    "field_na_officer_mobile",
    "field_na_officer_phone",
    "field_na_officer_phone_ext",
    "field_na_officer_photo",
    "field_na_officer_social",
    # An honorific, sometimes the full name instead, e.g. "HE", "mr.".
    "field_na_officer_prefix",
]


def pop_value(record: dict[str, Any], key: str, attr: str = "value") -> Any:
    items = record.pop(key)
    assert len(items) <= 1, (key, items)
    return items[0][attr] if len(items) == 1 else None


def fetch_agency(context: Context, url: str) -> dict[str, Any]:
    # The record is embedded in the Next.js payload pushed in the page's scripts.
    doc = zyte_api.fetch_html(
        context,
        url,
        "//script[contains(text(), 'field_na_officer_name')]",
        html_source="httpResponseBody",
        cache_days=10,
    )
    chunks = h.xpath_strings(
        doc, "//script[starts-with(text(), 'self.__next_f.push([1,')]/text()"
    )
    stream = "".join(
        json.loads(c[len("self.__next_f.push([1,") : -len("])")]) for c in chunks
    )
    # Rows aren't reliably newline-separated (free-text rows run into the next),
    # so decode the record object where it starts.
    marker = '{"data":{"nid":'
    assert stream.count(marker) == 1, url
    payload, _ = json.JSONDecoder().raw_decode(stream, stream.index(marker))
    record: dict[str, Any] = payload["data"]
    return record


def make_position_name(
    context: Context, raw_role: str | None, raw_name: str, agency: str
) -> str:
    """Name a head's position from the source's position text and the agency.

    The text becomes "Head" where it holds only an honorific or the head's own name,
    and the agency is appended unless the text already names it."""
    role = "Head"
    if raw_role is not None and squash_spaces(raw_role) != squash_spaces(raw_name):
        role = squash_spaces(raw_role)
        res = context.lookup("position_role", role)
        role = role if res is None else str(res.value)
    if agency.lower() in role.lower():
        return role
    return f"{role}, {agency}"


def crawl_agency(context: Context, url: str) -> None:
    record = fetch_agency(context, url)
    nid = str(pop_value(record, "nid"))
    title = squash_spaces(pop_value(record, "title"))
    raw_name = pop_value(record, "field_na_officer_name")
    raw_role = pop_value(record, "field_na_officer_position")
    context.audit_data(record, ignore=IGNORE)
    if context.lookup("test_agency", nid) is not None:
        return
    ministry = re.fullmatch(r"Ministry of (?P<portfolio>.+)", title, re.IGNORECASE)
    if context.lookup("not_minister", nid) is not None:
        ministry = None
    # "-" is a placeholder for no head.
    if raw_name is None or raw_name.strip() == "-":
        if ministry is not None:
            context.log.warning("Ministry has no head", url=url, title=title)
        return

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    name = h.strip_name_titles(context, raw_name)
    stripped = name != squash_spaces(raw_name)
    person.add("name", name, original_value=raw_name if stripped else None)
    person.add("sourceUrl", url)

    if ministry is not None:
        # Ministers must be Saudi nationals: Law of the Council of Ministers (1993),
        # Art. 3(a). https://hrlibrary.umn.edu/research/saudiarabia/law-council_ministries.html
        person.add("citizenship", "sa")
        # The source's position text is unreliable for ministers ("mr.", or two
        # offices), so the position is named from the ministry.
        position = h.make_position(
            context,
            name=f"Minister of {ministry.group('portfolio')} of Saudi Arabia",
            country="sa",
            topics=["gov.national", "gov.executive"],
            lang="eng",
        )
        categorisation = categorise(context, position)
    else:
        person.add("country", "sa")
        position = h.make_position(
            context,
            name=make_position_name(context, raw_role, raw_name, title),
            country="sa",
            lang="eng",
        )
        # The directory mixes political offices with universities, hospitals and
        # the like, so each position waits for categorisation in the review UI.
        categorisation = categorise(context, position, default_is_pep=None)
    if not categorisation.is_pep:
        return
    # The directory names each ministry's incumbent head, without dates.
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)


def sitemap_locs(context: Context, url: str) -> list[str]:
    _, _, _, text = zyte_api.fetch_text(context, url, expected_media_type=XML)
    doc = etree.fromstring(text.encode())
    return h.xpath_strings(doc, "//*[local-name()='loc']/text()")


def crawl(context: Context) -> None:
    # The directory's own listing pages overlap and miss agencies, so walk the
    # sitemap, which lists every agency page.
    urls: set[str] = set()
    for sitemap in sitemap_locs(context, context.data_url):
        urls.update(
            loc
            for loc in sitemap_locs(context, sitemap)
            if re.fullmatch(r"https://my\.gov\.sa/en/agencies/\d+", loc)
        )
    assert len(urls) > 0, "No agency pages in the sitemap"
    for url in sorted(urls):
        crawl_agency(context, url)
