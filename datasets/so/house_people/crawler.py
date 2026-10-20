import re
from urllib.parse import urljoin

from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.util import Element

# Only the ordinary member seat has a Wikidata item; the presiding offices have none.
POSITION_QIDS = {"Member of the House of the People of Somalia": "Q21328615"}
# Usually the bare seat number, sometimes prefixed "Seat No." or "Seat No:".
SEAT_NUMBER = re.compile(r"HOP#\d+")


def parse_fragment(fragment: str) -> Element:
    """Parse one of the rendered HTML snippets the API embeds in its JSON fields.
    The snippets have no single root element, and some are empty, so they are
    wrapped before parsing rather than handed to the parser as they are.
    """
    return html.fromstring(f"<div>{fragment}</div>")


def field_text(member: dict[str, str], key: str) -> str:
    """Return the text of a field the API renders as an HTML snippet.
    Every field is rendered for the page rather than carried as plain text, so
    even the ones that hold no markup are HTML-escaped and padded.
    """
    return h.element_text(parse_fragment(member[key]))


def crawl_member(context: Context, member_id: str, member: dict[str, str]) -> None:
    name = field_text(member, "name")
    # Vacated seats stay on the roster as placeholder records with the word "vacant"
    # worked into the member's name, still carrying some of the former holder's
    # details. Their successors are listed separately, as records of their own.
    if "vacant" in name.lower():
        context.log.info("Skipping vacant seat", name=name)
        return

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    clean_name = h.strip_name_titles(context, name)
    person.add("name", clean_name, original_value=name if clean_name != name else None)
    person.add("name", h.remove_bracketed(clean_name))
    person.add("email", h.element_text(parse_fragment(member["email"]).find(".//span")))
    # Members of the Federal Parliament must be Somali citizens: Provisional
    # Constitution of the Federal Republic of Somalia (2012), Art. 58(1)(a).
    # https://www.constituteproject.org/constitution/Somalia_2012
    person.add("citizenship", "so")
    # The plugin renders its extra fields unlabelled and in no fixed order, and the
    # field types are not used consistently, so each value is placed by its shape:
    # a seat number, a contact link on one record, otherwise a date of birth. Two
    # records carry a date field that was left blank.
    for field in parse_fragment(member["extra_fields"]).findall("./div"):
        value = h.element_text(field)
        if len(value) == 0 or SEAT_NUMBER.search(value):
            continue
        if value.lower().startswith("http"):
            continue
        h.apply_date(person, "birthDate", value)

    # The source names a federal member state rather than an electoral district:
    # the 275 seats are shared out between clans and filled by delegates nominated
    # in the states.
    constituency = context.lookup_value(
        "constituency", field_text(member, "location"), warn_unmatched=True
    )
    position = h.make_position(
        context,
        name="Member of the House of the People of Somalia",
        country="so",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328615",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    # The roster is maintained — it marks vacated seats and adds their
    # successors — but gives no dates, so an occupancy is undated and current.
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)
    context.emit(occupancy)
    context.emit(position)
    context.emit(person)


def crawl(context: Context) -> None:
    # The roster page renders only the first page of cards. The rest come from the
    # SP Team Pro plugin, whose REST route is named for a single member but, given
    # the id of the team widget rather than of a member, answers with every member
    # of that widget at once. The id is read from the page on each run: it is a
    # WordPress post id, which changes if the widget is rebuilt, and the route
    # answers an id it does not know with a 500 rather than an empty result.
    doc = context.fetch_html(context.data_url)
    widget_id = h.xpath_string(doc, ".//div[contains(@class, 'sp-team-pro')]/@data-id")
    url = urljoin(context.data_url, f"/wp-json/wp_team_pro/v1/sptp_member/{widget_id}")
    members = context.fetch_json(url)
    assert isinstance(members, dict), type(members)
    assert len(members) > 0, "Empty roster"
    # Photos, phone numbers, social links and the free-text description a few
    # members carry are deliberately not extracted.
    for member_id, member in members.items():
        assert isinstance(member_id, str), member_id
        assert isinstance(member, dict), member
        crawl_member(context, member_id, member)
