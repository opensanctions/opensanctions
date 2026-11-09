from typing import Any

from lxml import html
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h

ROLE_XPATH = ".//div[contains(@class, 'field--name-field-persona-desc')]"


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, Any],
) -> None:
    # e.g. <a href="http://parlamento.gub.uy/camarasycomisiones/legisladores/9917">Andrade, Oscar</a>
    link = html.fragment_fromstring(row.pop("Nombres"))
    url = h.xpath_string(link, "./@href")
    last_name, first_name = h.element_text(link).split(", ")
    party = row.pop("Lema").strip()
    department = row.pop("Departamento").strip()
    context.audit_data(row)

    profile = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=ROLE_XPATH,
        html_source="httpResponseBody",
        geolocation="UY",
        cache_days=7,
    )

    person = context.make("Person")
    person.id = context.make_slug(url.rsplit("/", 1)[-1])
    h.apply_name(person, first_name=first_name, last_name=last_name)
    # e.g. "Senador de la República por el Lema PARTIDO FRENTE AMPLIO", or a presiding
    # office such as "PRESIDENTE DE LA CÁMARA DE REPRESENTANTES - Representante ...".
    person.add("position", h.element_text(h.xpath_element(profile, ROLE_XPATH)))
    person.add("political", party)
    for mailto in h.xpath_strings(profile, ".//a[starts-with(@href, 'mailto:')]/@href"):
        person.add("email", mailto.removeprefix("mailto:"))
    person.add("sourceUrl", url)
    # Representatives and senators must be citizens: Constitution of Uruguay, Art. 90
    # and Art. 98. https://www.impo.com.uy/bases/constitucion/1967-1967
    person.add("citizenship", "uy")

    # The rosters list the members currently elected to each seat, including those on
    # leave, and give no dates. The per-legislature history on profiles is incomplete
    # for some members, so it is not used for dates.
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", department or None)
    context.emit(occupancy)
    context.emit(person)


def crawl_chamber(context: Context, url: str, position: Entity) -> None:
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    for row in zyte_api.fetch_json(context, url, geolocation="UY"):
        crawl_member(context, position, categorisation, row)


def crawl(context: Context) -> None:
    crawl_chamber(
        context,
        context.data_url,
        h.make_position(
            context,
            name="Senator of Uruguay",
            wikidata_id="Q19930721",
            country="uy",
            topics=["gov.national", "gov.legislative"],
            lang="eng",
        ),
    )
    crawl_chamber(
        context,
        "https://parlamento.gub.uy/camarasycomisiones/representantes/plenario/integracion/titulares/json?page&_format=json",
        h.make_position(
            context,
            name="Member of the Chamber of Representatives of Uruguay",
            wikidata_id="Q19930720",
            country="uy",
            topics=["gov.national", "gov.legislative"],
            lang="eng",
        ),
    )
