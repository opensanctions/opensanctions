from typing import Any
import orjson

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.extract.zyte_api import ZyteAPIRequest
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_deputy(
    context: Context,
    deputy: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    first_name = deputy.pop("nombres")
    last_name = deputy.pop("apellidos")

    person = context.make("Person")
    person.id = context.make_id(deputy.pop("id_diputado"), first_name, last_name)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    h.apply_date(person, "birthDate", deputy.pop("fecha_nacimiento"))

    # Deputies must be Guatemalan by origin ("guatemalteco de origen"); naturalised
    # citizens are not eligible (Political Constitution of Guatemala, Art. 162).
    # https://www.constituteproject.org/constitution/Guatemala_1993
    person.add("citizenship", "gt")
    person.add("political", deputy.pop("nombre_bloque"))
    if deputy["cv"] is not None:
        person.add(
            "sourceUrl",  # link to CV
            "https://www.congreso.gob.gt/assets/uploads/diputados/cv_pdf/"
            + deputy.pop("cv"),
        )

    # a single field sometimes packs two addresses, e.g. "x@gmail.com / y@org.gt".
    for email in h.multi_split(deputy.pop("email"), ["/"]):
        person.add("email", email)
    person.add("address", deputy.pop("direccion"))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", deputy.pop("nombre_distrito"))

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    zyte_result = zyte_api.fetch(
        context,
        ZyteAPIRequest(
            # the site is behind an Imperva/Incapsula firewall
            url=context.data_url,
            method="POST",
            headers={
                "Referer": "https://www.congreso.gob.gt/buscador_diputados",
                "X-Requested-With": "XMLHttpRequest",
            },
            geolocation="gt",
        ),
        cache_days=30,
    )
    deputies = orjson.loads(zyte_result.response_text)

    position = h.make_position(
        context,
        name="Member of the Congress of the Republic of Guatemala",
        country="gt",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18277108",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for deputy in deputies:
        crawl_deputy(context, deputy, position, categorisation)
