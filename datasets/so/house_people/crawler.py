from urllib.parse import urljoin

from lxml import html

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

Positions = dict[str, tuple[Entity, PositionCategorisation]]


def parse_fragment(fragment: str) -> Element:
    """Parse an API field's HTML snippet, which may be empty or have no root."""
    return html.fromstring(f"<div>{fragment}</div>")


def field_text(fragment: str) -> str:
    """Return the text of an API field, which is HTML-escaped even without markup."""
    return h.element_text(parse_fragment(fragment))


def get_position(
    context: Context, positions: Positions, name: str
) -> tuple[Entity, PositionCategorisation]:
    if name not in positions:
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
        positions[name] = (position, categorise(context, position))
    return positions[name]


def crawl_member(
    context: Context, positions: Positions, member_id: str, member: dict[str, str]
) -> None:
    name = field_text(member.pop("name"))
    # Vacated seats stay on the roster as placeholders; successors get their own card.
    if "vacant" in name.lower():
        context.log.info("Skipping vacant seat", name=name)
        return

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    clean_name = h.strip_name_titles(context, name)
    person.add("name", clean_name, original_value=name if clean_name != name else None)
    # Bracketed nicknames.
    unbracketed = h.remove_bracketed(clean_name)
    if unbracketed != clean_name:
        person.add("name", unbracketed)
    # Provisional Constitution (2012), Art. 58(1)(a):
    # https://www.constituteproject.org/constitution/Somalia_2012
    person.add("citizenship", "so")
    # Text fields hold the seat number, or on five cards the date of birth; the
    # number field is a WhatsApp link.
    for field in h.xpath_elements(parse_fragment(member.pop("extra_fields")), "./div"):
        kind = field.get("class")
        value = h.element_text(field)
        if kind == "sptp-member-extra-info sptp-member-extra-info-text":
            if "HOP#" not in value:
                h.apply_date(person, "birthDate", value)
        elif kind == "sptp-member-extra-info sptp-member-extra-info-date":
            h.apply_date(person, "birthDate", value)
        elif kind != "sptp-member-extra-info sptp-member-extra-info-number":
            raise ValueError(f"Unknown extra field {kind!r}: {value!r}")

    # A federal member state, not an electoral district.
    constituency = context.lookup_value(
        "constituency", field_text(member.pop("location")), warn_unmatched=True
    )
    job_title = field_text(member.pop("job_title"))
    res = context.lookup("position", job_title, warn_unmatched=True)
    if res is None:
        return
    for title in res.values:
        position, categorisation = get_position(context, positions, title)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
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
    positions: Positions = {}
    for member_id, member in members.items():
        crawl_member(context, positions, member_id, member)
