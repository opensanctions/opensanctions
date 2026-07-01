from typing import Any

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import categorise

DATA_JSON_URL = "http://duma.gov.ru/data.json"


def current_fraction(record: dict[str, Any], convocation: int) -> dict[str, Any] | None:
    matches = [
        fraction
        for fraction in record["fraction_positions"]
        if fraction["convocation"] == convocation and fraction["actual"]
    ]
    if len(matches) > 1:
        raise ValueError(
            f"Person {record['id']} has multiple current fraction memberships "
            f"for convocation {convocation}"
        )
    return matches[0] if matches else None


def crawl_person(
    context: Context,
    position: Entity,
    record: dict[str, Any],
    fraction: dict[str, Any],
) -> None:
    person_id = record["id"]
    if not record["first_name"] or not record["last_name"]:
        raise ValueError(f"Deputy {person_id} has no name in data.json")

    person = context.make("Person")
    person.id = context.make_slug(str(person_id))
    h.apply_name(
        person,
        first_name=record["first_name"],
        patronymic=record["second_name"] or None,
        last_name=record["last_name"],
        lang="rus",
    )

    # Russian citizenship is required for a State Duma seat, and holding foreign
    # citizenship or a foreign residence permit disqualifies a candidate, per
    # Article 97(1) of the Constitution of the Russian Federation:
    # https://constitutionrf.ru/rzd-1/gl-5/st-97-krf
    person.add("citizenship", "ru")
    person.add("sourceUrl", f"http://duma.gov.ru/duma/persons/{person_id}/")

    # IMPORTANT: all person props must be set before make_occupancy
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return

    # data.json gives no per-person date, so the occupancy is dateless and
    # relies on no_end_implies_current (the default) to resolve to "current".
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    occupancy.add("politicalGroup", fraction["org_title"])
    occupancy.add("constituency", fraction["regions_title"] or None)

    context.emit(position)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    data = context.fetch_json(DATA_JSON_URL, cache_days=1)
    convocation = data["last_convocation"]

    # Q17276321 — member of the State Duma, the lower house of the Federal
    # Assembly of Russia.
    position = h.make_position(
        context,
        name="Member of the State Duma",
        country="ru",
        wikidata_id="Q17276321",
        topics=["gov.national", "gov.legislative"],
    )

    for record in data["persons"]:
        fraction = current_fraction(record, convocation)
        if fraction is not None:
            crawl_person(context, position, record, fraction)
