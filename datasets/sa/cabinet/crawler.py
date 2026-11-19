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

MINISTRIES = 17489  # The "Ministries" agency type.
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
    # The head's office contacts and photo; phone numbers are not extracted.
    "field_na_officer_email",
    "field_na_officer_mobile",
    "field_na_officer_phone",
    "field_na_officer_phone_ext",
    "field_na_officer_photo",
    "field_na_officer_social",
    # An honorific, sometimes the full name instead, e.g. "HE", "mr.".
    "field_na_officer_prefix",
    # Unreliable: sometimes only an honorific ("Doctor"), sometimes several offices.
    # The position is named from the ministry instead.
    "field_na_officer_position",
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
    starts = [m.start() for m in re.finditer(re.escape('{"data":{"nid":'), stream)]
    assert len(starts) == 1, url
    payload, _ = json.JSONDecoder().raw_decode(stream, starts[0])
    record: dict[str, Any] = payload["data"]
    return record


def crawl_agency(context: Context, url: str) -> None:
    record = fetch_agency(context, url)
    nid = pop_value(record, "nid")
    if pop_value(record, "field_na_type", "target_id") != MINISTRIES:
        return
    title = squash_spaces(pop_value(record, "title"))
    raw_name = pop_value(record, "field_na_officer_name")
    context.audit_data(record, ignore=IGNORE)
    if context.lookup_value("not_ministry", str(nid)) is not None:
        return
    match = re.match(r"^Ministry of (?P<portfolio>.+)$", title, re.IGNORECASE)
    if match is None:
        context.log.warning("Unexpected ministry title", url=url, title=title)
        return
    if raw_name is None:
        context.log.warning("Ministry has no head", url=url, title=title)
        return

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    name = h.strip_name_titles(context, raw_name)
    stripped = name != squash_spaces(raw_name)
    person.add("name", name, original_value=raw_name if stripped else None)
    # Ministers must be Saudi nationals: Law of the Council of Ministers (1993),
    # Art. 3(a). https://hrlibrary.umn.edu/research/saudiarabia/law-council_ministries.html
    person.add("citizenship", "sa")
    person.add("sourceUrl", url)

    position = h.make_position(
        context,
        name=f"Minister of {match.group('portfolio')} of Saudi Arabia",
        country="sa",
        topics=["gov.national", "gov.executive"],
        lang="eng",
    )
    categorisation = categorise(context, position)
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
