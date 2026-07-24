from collections.abc import Iterator
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

GENDERS = {"M": "male", "F": "female"}
TOPICS = ["gov.national", "gov.legislative"]


def next_page_url(data: dict[str, Any]) -> str | None:
    """Return the href of the API response's `next` page link, if there is one."""
    return next((link["href"] for link in data["links"] if link["rel"] == "next"), None)


def fetch_paginated(
    context: Context, url: str, params: dict[str, Any]
) -> Iterator[dict[str, Any]]:
    """Yield every item across the API's result pages.

    The API caps a page at 1000 items regardless of the requested `itens`, so a
    larger result set has to be walked page by page by following the `next` link.
    """
    data = context.fetch_json(url, params={**params, "itens": 1000}, cache_days=1)
    yield from data["dados"]
    next_url = next_page_url(data)
    while next_url is not None:
        data = context.fetch_json(next_url, cache_days=1)
        yield from data["dados"]
        next_url = next_page_url(data)


def crawl_deputy(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    deputy_id: int,
    term_start: str,
    term_end: str,
    legislature_id: int,
) -> None:
    detail = context.fetch_json(
        f"{context.data_url}/deputados/{deputy_id}", cache_days=14
    )["dados"]
    status = detail.pop("ultimoStatus")

    person = context.make("Person")
    person.id = context.make_slug(str(deputy_id))
    person.add("name", status.pop("nome"), lang="por")
    person.add("name", detail.pop("nomeCivil"), lang="por")
    person.add("gender", GENDERS.get(detail.pop("sexo")))
    h.apply_date(person, "birthDate", detail.pop("dataNascimento"))
    h.apply_date(person, "deathDate", detail.pop("dataFalecimento"))
    municipality = detail.pop("municipioNascimento")
    uf = detail.pop("ufNascimento")
    if municipality and uf:
        person.add("birthPlace", f"{municipality}, {uf}", lang="por")
    person.add("taxNumber", detail.pop("cpf"))
    person.add("website", detail.pop("urlWebsite"))
    person.add("sourceUrl", detail.pop("uri"))
    # Federal deputies must be Brazilian nationals (Constitution of Brazil 1988,
    # Article 14 §3 I). https://www.constituteproject.org/constitution/Brazil_2017
    person.add("citizenship", "br")

    party = status.pop("siglaPartido")
    constituency = status.pop("siglaUf")
    situacao = status.pop("situacao")
    status_date = status.pop("data")
    # ultimoStatus only describes the deputy's most recent term.
    is_most_recent = status.pop("idLegislatura") == legislature_id
    context.audit_data(
        detail,
        ignore=["id", "redeSocial", "escolaridade"],
    )
    context.audit_data(
        status,
        ignore=[
            "id",
            "uri",
            "uriPartido",
            "urlFoto",
            "email",
            "nomeEleitoral",
            "gabinete",
            "condicaoEleitoral",
            "descricaoStatus",
        ],
    )

    # The legislature's term is the period we can always attribute to an occupancy.
    # Only for the deputy's current term ("Exercício") do we also know a personal
    # entry date and that the occupancy is still open; past terms keep just the
    # period bounds, since we don't know when the deputy personally entered or left.
    holds_now = is_most_recent and situacao == "Exercício"
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=status_date if holds_now else None,
        period_start=term_start,
        period_end=term_end,
        no_end_implies_current=holds_now,
    )
    if occupancy is None:
        return
    if is_most_recent:
        occupancy.add("politicalGroup", party)
        occupancy.add("constituency", constituency)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Chamber of Deputies of Brazil",
        country="br",
        wikidata_id="Q20058725",
        topics=TOPICS,
        number_of_seats="513",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    cutoff = h.earliest_term_start(TOPICS)
    legislatures = sorted(
        fetch_paginated(
            context,
            f"{context.data_url}/legislaturas",
            {"ordenarPor": "id", "ordem": "DESC"},
        ),
        key=lambda legislature: int(legislature["id"]),
        reverse=True,
    )

    for legislature in legislatures:
        # Terms are processed newest-first; once one ended before the PEP cut-off,
        # every older term is out of scope too.
        if legislature["dataFim"] < cutoff:
            break
        deputies = fetch_paginated(
            context,
            f"{context.data_url}/deputados",
            {"idLegislatura": legislature["id"], "ordenarPor": "nome", "ordem": "ASC"},
        )
        for deputy in deputies:
            crawl_deputy(
                context,
                position,
                categorisation,
                deputy["id"],
                legislature["dataInicio"],
                legislature["dataFim"],
                legislature["id"],
            )
