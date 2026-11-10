import re
from typing import Any
from urllib.parse import urljoin

from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h

LEGISLATURE_LABEL = re.compile(r"^(\d{4})-(\d{4})$")


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    constituency: str | None,
    period_start: str,
    period_end: str | None,
    row: dict[str, Any],
) -> None:
    profile_url = row.pop("Perfil")

    person = context.make("Person")
    person.id = context.make_id(profile_url)
    person.add("name", row.pop("Nombre"))
    person.add("sourceUrl", profile_url)
    party = row.pop("Partido")
    party_lookup = context.lookup("political", party)
    person.add("political", party if party_lookup is None else party_lookup.value)
    person.add("citizenship", "co")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=period_start,
        period_end=period_end,
        # Only the sitting year is open; a past year not yet over is unknown.
        no_end_implies_current=period_end is None,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)
    context.emit(occupancy)
    context.emit(person)

    context.audit_data(
        row,
        ignore=[
            # Not extracted for PEPs.
            "Correo",
            "Telefono",
            "Oficina",
            "Imagen",
            # No matching property.
            "Votos",
            "Comision",
            "Comisiones",
            "IdComision",
        ],
    )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Chamber of Representatives of Colombia",
        wikidata_id="Q18960607",
        country="co",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url)
    ajax_url = urljoin(context.data_url, "/wp-admin/admin-ajax.php")
    for option in h.xpath_elements(doc, "//select[@id='select-legislatura']/option"):
        label = h.element_text(option)
        match = LEGISLATURE_LABEL.match(label)
        if match is None:
            context.log.warning("Unexpected legislative year label", label=label)
            continue
        start_year, end_year = match.groups()

        data = context.fetch_json(
            ajax_url,
            method="POST",
            data={
                "action": "cargar_representantes_legislatura",
                "legislatura": option.get("value"),
            },
        )["data"]
        is_current = data["es_mas_reciente"]
        # Fails while the site is switching to a new legislative year.
        assert is_current is (option.get("selected") is not None), label

        # Some years before 2014 have no members.
        if data["departamentos"] == []:
            if is_current:
                context.log.warning(
                    "Sitting legislative year has no members", label=label
                )
            else:
                context.log.info(
                    "No members published for legislative year", label=label
                )
            continue
        for key, members in data["departamentos"].items():
            key_lookup = context.lookup("constituency", key)
            constituency = key if key_lookup is None else key_lookup.value
            for row in members:
                crawl_member(
                    context,
                    position,
                    categorisation,
                    constituency,
                    start_year,
                    None if is_current else end_year,
                    row,
                )
