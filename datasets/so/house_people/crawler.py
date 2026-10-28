from functools import cache
from html import unescape
from urllib.parse import urljoin

from lxml import html

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)


@cache
def get_position(context: Context, name: str) -> tuple[Entity, PositionCategorisation]:
    position = h.make_position(
        context,
        name=name,
        country="so",
        topics=["gov.national", "gov.legislative"],
        # The presiding offices have no Wikidata item.
        wikidata_id=(
            "Q21328615"
            if name == "Member of the House of the People of Somalia"
            else None
        ),
        lang="eng",
    )
    return position, categorise(context, position)


def crawl_member(context: Context, member_id: str, member: dict[str, str]) -> None:
    name = member.pop("name")
    # Vacated seats stay on the roster as placeholder cards, some naming the former
    # holder, whose mandate has ended at an undisclosed date.
    status = None
    if "vacant" in name.lower():
        former = context.lookup_value("vacant_seat", name, warn_unmatched=True)
        if former is None:
            return
        name = former
        status = OccupancyStatus.ENDED

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    clean_name = h.strip_name_titles(context, name)
    person.add("name", clean_name, original_value=name if clean_name != name else None)
    # Bracketed nicknames.
    unbracketed = h.remove_bracketed(clean_name)
    if unbracketed != clean_name:
        person.add("name", unbracketed)
    person.add("citizenship", "so")
    # Besides the seat number and a WhatsApp link (number field), the extra fields
    # hold the date of birth, on five cards typed into a text field.
    for field in html.fragments_fromstring(
        member.pop("extra_fields"), no_leading_text=True
    ):
        value = h.element_text(field)
        if "HOP#" in value or field.get("class", "").endswith("-number"):
            continue
        h.apply_date(person, "birthDate", value)

    # A federal member state, not an electoral district.
    location = member.pop("location")
    res = context.lookup("constituency", location)
    constituency = location if res is None else res.value
    # HTML-escaped, though without markup.
    job_title = unescape(member.pop("job_title"))
    res = context.lookup("position", job_title, warn_unmatched=True)
    if res is None:
        return
    for title in res.values:
        position, categorisation = get_position(context, title)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation, status=status
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", constituency)
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)

    context.audit_data(
        member,
        ignore=[
            "email",
            "phone",
            "mobile",
            "social",
            "thumbs",
            "website",
            "experience",
            "desc",
            "skills",
            "author_post",
        ],
    )


def crawl(context: Context) -> None:
    # The page renders only the first cards. Given the team widget's id, the
    # plugin's member route returns the whole roster; the id is read per run as it
    # changes if the widget is rebuilt.
    doc = context.fetch_html(context.data_url)
    widget_id = h.xpath_string(doc, ".//div[contains(@class, 'sp-team-pro')]/@data-id")
    url = urljoin(context.data_url, f"/wp-json/wp_team_pro/v1/sptp_member/{widget_id}")
    members = context.fetch_json(url)
    for member_id, member in members.items():
        crawl_member(context, member_id, member)
