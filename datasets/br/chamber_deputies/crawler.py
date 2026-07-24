from itertools import count
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

GENDERS = {"M": "male", "F": "female"}


def crawl_deputy(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    summary: dict[str, Any],
) -> None:
    deputy_id = summary["id"]
    detail = context.fetch_json(f"{context.data_url}/{deputy_id}", cache_days=14)[
        "dados"
    ]

    person = context.make("Person")
    person.id = context.make_slug(str(deputy_id))
    person.add("name", summary.get("nome"), lang="por")
    person.add("name", detail.get("nomeCivil"), lang="por")
    person.add("gender", GENDERS.get(detail.get("sexo")))
    h.apply_date(person, "birthDate", detail.get("dataNascimento"))
    h.apply_date(person, "deathDate", detail.get("dataFalecimento"))
    municipality = detail.get("municipioNascimento")
    uf = detail.get("ufNascimento")
    if municipality and uf:
        person.add("birthPlace", f"{municipality}, {uf}", lang="por")
    person.add("political", summary.get("siglaPartido"), lang="por")
    person.add("taxNumber", detail.get("cpf"))
    person.add("sourceUrl", summary.get("uri"))
    # Federal deputies must be Brazilian nationals (Constitution of Brazil 1988,
    # Article 14 §3 I). https://www.constituteproject.org/constitution/Brazil_2017
    person.add("citizenship", "br")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    if summary.get("siglaUf"):
        occupancy.add("constituency", summary["siglaUf"])
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Chamber of Deputies of Brazil",
        country="br",
        wikidata_id="Q20058725",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    seen = 0
    for page in count(1):
        data = context.fetch_json(
            context.data_url,
            params={"ordem": "ASC", "ordenarPor": "nome", "itens": 100, "pagina": page},
            cache_days=1,
        )
        deputies = data["dados"]
        if not deputies:
            break
        for summary in deputies:
            crawl_deputy(context, position, categorisation, summary)
            seen += 1

    if seen == 0:
        raise ValueError("No deputies returned by the Chamber API")
