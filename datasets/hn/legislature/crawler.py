import json
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Fields not emitted: the official work email, photo, the internal record id ("dni" — a
# 1-6 digit site id, not a real national identity number), the electoral department, the
# formula/priority ordering, and the timestamp.
# `firstName`/`lastName`/`delegate_id` are redundant duplicates of nombres/apellidos/dni
# present on some records.
IGNORE_FIELDS = [
    "titulo",
    "departamento",
    "email",
    "img",
    "formula",
    "priority",
    "createAt",
    "firstName",
    "lastName",
    "delegate_id",
]


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    # Only titular deputies (propietarios) are emitted; their alternates are suplentes.
    cargo = row.pop("cargo")
    if cargo != "PROPIETARIO":
        return

    record_id = row.pop("dni")
    nombres = row.pop("nombres")
    apellidos = row.pop("apellidos")
    bancada = row.pop("bancada")

    person = context.make("Person")
    person.id = context.make_slug(str(record_id))
    h.apply_name(person, first_name=nombres, last_name=apellidos)
    # Deputies must be Honduran by birth ("hondureño por nacimiento"); naturalised
    # citizens are not eligible (Constitution of Honduras, Art. 198).
    # https://pdba.georgetown.edu/Constitutions/Honduras/hond82.html
    person.add("citizenship", "hn")
    person.add("political", bancada)

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
    context.audit_data(row, ignore=IGNORE_FIELDS)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    # The deputy roster is embedded in the Next.js page data; parsing it (rather than the
    # versioned _next/data endpoint) keeps the crawler independent of the build id.
    script = h.xpath_element(doc, ".//script[@id='__NEXT_DATA__']")
    if script.text is None:
        raise ValueError("Empty __NEXT_DATA__ payload")
    data = json.loads(script.text)
    deputies = data["props"]["pageProps"]["congresistastemp"]["congresistas"]
    if not isinstance(deputies, list) or len(deputies) < 100:
        raise ValueError("Unexpected deputy payload: %r" % type(deputies))

    position = h.make_position(
        context,
        name="Member of the National Congress of Honduras",
        country="hn",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q19300340",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    for row in deputies:
        crawl_deputy(context, row, position, categorisation)
