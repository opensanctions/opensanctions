from typing import Any, Dict

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# The official site (asamblea.gob.pa) is behind an F5 BIG-IP JavaScript challenge, so
# every page must be fetched through Zyte browser rendering with a Panama geolocation.
GEOLOCATION = "pa"

# TODO(zyte): once you can render the page, replace these placeholders with the real
# XPaths discovered from the rendered DOM (`zavod crawl` writes the fetched HTML under
# data/datasets/pa_asamblea/ when you add a debug dump, or inspect via the Zyte UI).
#
# `DEPUTIES_XPATH` must match one element per deputy AND nothing on the challenge page,
# so it doubles as the `unblock_validator` (see fetch_html below). Point it at the
# repeating deputy card/row in the directory grid.
DEPUTIES_XPATH = ".//div[contains(@class, 'TODO-deputy-card')]"

# Field XPaths are evaluated *relative* to a single deputy element (note the leading "./").
NAME_XPATH = "./TODO"  # e.g. ".//*[contains(@class, 'nombre')]"
PARTY_XPATH = "./TODO"  # e.g. ".//*[contains(@class, 'partido')]"
# A link to the deputy's profile or an element carrying a stable numeric id; used to key
# the entity. If the site exposes no id, fall back to context.make_id() on name+circuito.
PROFILE_LINK_XPATH = "./TODO/@href"  # e.g. ".//a/@href"


def wait_for_xpath_actions(xpath: str) -> list[Dict[str, Any]]:
    """Zyte browser actions: wait for navigation to settle and for the deputy grid
    (which is populated client-side via XHR) to become visible before snapshotting."""
    return [
        {
            "action": "waitForNavigation",
            "waitUntil": "networkidle0",
            "timeout": 31,
            "onError": "return",
        },
        {
            "action": "waitForSelector",
            "selector": {"type": "xpath", "value": xpath, "state": "visible"},
            "timeout": 15,
            "onError": "return",
        },
    ]


def crawl_deputy(
    context: Context,
    card: Element,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    name = h.element_text(h.xpath_element(card, NAME_XPATH))
    party = h.element_text(h.xpath_element(card, PARTY_XPATH))
    # TODO(zyte): extract birth date if the card/profile carries one, then
    #   h.apply_date(person, "birthDate", born)  # ISO "YYYY-MM-DD"

    person = context.make("Person")
    # TODO(zyte): key on the official deputy id parsed from PROFILE_LINK_XPATH.
    # Fallback if no id is available:
    #   person.id = context.make_id(name, party)
    profile = h.xpath_string(card, PROFILE_LINK_XPATH)
    person.id = context.make_slug(profile.rstrip("/").rsplit("/", 1)[-1])
    person.add("name", name)
    # Deputies must be Panamanian — by birth, or naturalised with fifteen years'
    # residence (Political Constitution of Panama, Art. 153).
    # https://constitucion.te.gob.pa/organo-legislativo/
    person.add("citizenship", "pa")
    # Party as published; deputies elected "por la libre" / independents may have none.
    person.add("political", party)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=DEPUTIES_XPATH,
        actions=wait_for_xpath_actions(DEPUTIES_XPATH),
        html_source="browserHtml",
        geolocation=GEOLOCATION,
        cache_days=30,
    )

    cards = h.xpath_elements(doc, DEPUTIES_XPATH)
    if len(cards) < 50:
        raise ValueError("Expected at least 50 deputies, got %d" % len(cards))

    position = h.make_position(
        context,
        name="Member of the National Assembly of Panama",
        country="pa",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295996",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    for card in cards:
        crawl_deputy(context, card, position, categorisation)


# ---------------------------------------------------------------------------------------
# ALTERNATIVE (likely cleaner) — fetch the underlying JSON API instead of the DOM.
#
# The backend is ASP.NET Web API: `/api/*` returns real JSON and is NOT behind the F5
# challenge (a bad route returns the ASP.NET "No HTTP resource was found..." message,
# not the challenge page). The /Diputados SPA fetches its grid from some `/api/{controller}`
# endpoint. To find it: render /Diputados via Zyte with the browser network log, or open
# the page in a real browser and watch the Network tab for the deputies XHR.
#
# Once known, replace crawl() above with a JSON fetch and map fields directly:
#
#   data = zyte_api.fetch_json(context, API_URL, geolocation="pa", cache_days=30)
#   for row in data["TODO_key"]:
#       if row.get("activo") is not True:   # skip inactive, if the API marks them
#           continue
#       # skip alternates (suplentes) — they are different, non-sitting persons
#       person = context.make("Person")
#       person.id = context.make_slug(str(row["id"]))
#       person.add("name", row["nombreCompleto"])
#       h.apply_date(person, "birthDate", row["borndate"][:10])  # ISO timestamp -> date
#       person.add("citizenship", "pa")
#       person.add("political", row["partido"])
#       ... (make_occupancy / emit / audit_data as above)
# ---------------------------------------------------------------------------------------
