from itertools import count

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_json, fetch_html
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Roster lives in a WordPress "diputados" custom post type; data_url is its REST
# collection endpoint. Each post links to a profile page whose structured fields
# (party, department, titular/suplente) are only rendered in the page HTML.
PAGE_SIZE = 100


def deputy_fields(doc: HtmlElement) -> dict[str, str]:
    """Map each profile's Elementor icon-box label to its value.

    Profiles render one icon-box per attribute, with the label in the box title and
    the value in the box description (e.g. "Bancada" -> "MAS IPSP ...").
    """
    fields: dict[str, str] = {}
    for box in h.xpath_elements(
        doc, ".//div[contains(@class, 'elementor-icon-box-content')]"
    ):
        titles = h.xpath_elements(
            box, ".//*[contains(@class, 'elementor-icon-box-title')]"
        )
        if not titles:
            continue
        descriptions = h.xpath_elements(
            box, ".//*[contains(@class, 'elementor-icon-box-description')]"
        )
        label = h.element_text(titles[0])
        fields[label] = h.element_text(descriptions[0]) if descriptions else ""
    return fields


def crawl_deputy(
    context: Context,
    url: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    html_response = fetch_html(
        context,
        url,
        ".//div[contains(@class, 'elementor-icon-box-content')]",
        geolocation="bo",
        cache_days=7,
    )
    fields = deputy_fields(html_response)

    name = fields.pop("Nombre", "").strip()
    if len(name) == 0:
        context.log.warning("Deputy profile missing name", url=url)
        return
    department = fields.pop("Brigada", None)

    person = context.make("Person")
    person.id = context.make_id(name, department)
    h.apply_name(person, full=name)
    h.apply_date(person, "birthDate", fields.pop("Fecha de nacimiento", None))
    person.add("email", fields.pop("Correo electronico", None))
    # Deputies sit in the Asamblea Legislativa Plurinacional, whose membership is
    # reserved to Bolivian citizens (Constitution arts. 144, 149-150).
    # https://pdba.georgetown.edu/Constitutions/Bolivia/bolivia09.html
    person.add("citizenship", "bo")
    person.add("political", fields.pop("Bancada", None))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    # Deputies are elected by department, via single-member districts, party lists or
    # special indigenous-campesino circumscriptions (the seat type, "Diputación").
    occupancy.add("constituency", department)

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(
        fields,
        ignore=[
            # The site rarely fills the titular/suplente flag (blank on ~90% of
            # profiles), so it can't be used to limit the roster to titulares; both
            # are emitted. The label is gendered (Legislador / Legisladora).
            "Legislador",
            "Legisladora",
            "Suplente",  # the deputy's own alternate's name, where filled
            "Diputación",  # seat type (uninominal / plurinominal / especial)
            "Circunscripción",  # single-member district number, where applicable
            "Comisión",  # committee assignments
            "Comité",
            "Cargo comisión",
        ],
    )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Chamber of Deputies of Bolivia",
        country="bo",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q20081432",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for page in count(1):
        url = f"{context.data_url}?per_page={PAGE_SIZE}&page={page}"
        records = fetch_json(
            context,
            url,
            cache_days=1,
            geolocation="bo",
        )
        for record in records:
            crawl_deputy(context, record["link"], position, categorisation)
        # The REST collection is paginated; the final page is short of PAGE_SIZE.
        if len(records) < PAGE_SIZE:
            break
