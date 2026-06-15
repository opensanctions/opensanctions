import re
from typing import Any

from lxml.html import HtmlElement, fromstring
from normality import squash_spaces

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import OccupancyStatus, PositionCategorisation, categorise

# Deep link to a member's profile on the official site, built from the API `slug`.
PROFILE_URL = "https://www.skupstina.me/me/clanovi-parlamenta/%s"


def clean_date(value: str | None) -> str | None:
    """Tidy the source's free-text date of birth before date parsing.

    The ``date_of_birth`` field mixes numeric and Montenegrin-month-name forms and
    sometimes carries a trailing "godine" ("of the year") or stray double spaces; we
    normalise whitespace and drop the suffix so the dataset date formats can match.
    """
    if value is None:
        return None
    value = squash_spaces(value)
    if value.lower().endswith("godine"):
        value = value[: -len("godine")].strip()
    return value or None


# Block-level tags that should become line breaks when flattening the biography HTML,
# so paragraphs don't run together (e.g. "2020.IZBORNA LISTA").
BLOCK_BREAK = re.compile(r"</(p|div|li|h[1-6]|tr|ul|ol)>|<br\s*/?>", re.IGNORECASE)


def html_to_text(value: str | None) -> str | None:
    """Flatten an HTML biography snippet to readable plain text.

    Block boundaries are turned into line breaks before tags are stripped, then each
    line is whitespace-squashed and empty lines dropped.
    """
    if value is None or not value.strip():
        return None
    element: HtmlElement = fromstring(BLOCK_BREAK.sub("\n", value))
    lines = [squash_spaces(line) for line in element.text_content().split("\n")]
    return "\n".join(line for line in lines if line) or None


def make_member_position(context: Context) -> Entity:
    return h.make_position(
        context,
        name="Member of the Parliament of Montenegro",
        country="me",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328576",
    )


def crawl_member(
    context: Context,
    member: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    translation = member.pop("translation")
    person = context.make("Person")
    person.id = context.make_slug("person", member.pop("id"))
    h.apply_name(person, full=translation.pop("name"), lang="cnr")
    h.apply_date(person, "birthDate", clean_date(translation.pop("date_of_birth")))
    person.add("birthPlace", translation.pop("place_of_birth"), lang="cnr")
    person.add("biography", html_to_text(translation.pop("biography")), lang="cnr")
    # Standing for the Skupština requires Montenegrin citizenship: Constitution of
    # Montenegro Art. 45, implemented by the Law on Election of Councillors and MPs
    # Art. 2. https://www.constituteproject.org/constitution/Montenegro_2013
    person.add("citizenship", "me")

    party = member.pop("political_party", None)
    if party is not None:
        person.add("political", party["translation"]["name"], lang="cnr")

    # term_of_office_ceased is currently always 0; treat either flag as "no longer in
    # office" so the crawler keeps working if the source starts using it.
    term_expired = member.pop("term_expired")
    term_ceased = member.pop("term_of_office_ceased")
    is_former = term_expired == 1 or term_ceased == 1
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        no_end_implies_current=not is_former,
        status=OccupancyStatus.ENDED if is_former else None,
    )
    if occupancy is None:
        return

    group = member.pop("parliamentary_group", None)
    if group is not None:
        occupancy.add("politicalGroup", group["translation"]["name"], lang="cnr")
    occupancy.add("sourceUrl", PROFILE_URL % translation.pop("slug"))

    context.audit_data(
        member,
        ignore=[
            "image_id",
            "featured_image_id",
            "image",
            # Leadership flags: we model everyone as a plain member.
            "president",
            "vice_president",
            "seat_number",
            "political_party_id",
            "parliamentary_group_id",
            "creator_id",
            "created_at",
            "updated_at",
        ],
    )
    context.audit_data(
        translation,
        ignore=[
            "id",
            "parliament_member_id",
            "locale",
            "status",
            "electoral_list",
            # Contact and committee-membership detail we deliberately drop.
            "email",
            "phone",
            "fax",
            "address",
            "parliamentary_function",
            "seo_title",
            "seo_keywords",
            "seo_description",
            "editor_id",
            "created_at",
            "updated_at",
        ],
    )
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url, cache_days=1)
    members = data["data"]["data"]
    # The API returns everything in one page; guard against silent truncation if the
    # roster ever grows past the limit set in the data URL.
    assert 0 < len(members) < 500, len(members)

    position = make_member_position(context)
    context.emit(position)
    categorisation = categorise(context, position, default_is_pep=True)

    for member in members:
        crawl_member(context, member, position, categorisation)
