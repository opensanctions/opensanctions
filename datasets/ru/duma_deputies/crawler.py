from typing import Any

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import categorise
from zavod.util import Element

BASE_URL = "http://duma.gov.ru/duma/persons/"


def current_fraction(record: dict[str, Any], convocation: int) -> dict[str, Any] | None:
    # fraction_positions holds a person's full history across every convocation
    # they've served in, and can even list two rows for the same convocation
    # when a mid-term fraction change occurred. `actual` marks the one that's
    # true right now, so this just picks that latest row rather than a stale
    # one — it does not exclude any currently seated deputy: those with no
    # party affiliation still have a row here for the "not in a faction"
    # pseudo-fraction, so None only means the person isn't seated this term.
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


def extract_details(doc: Element) -> tuple[str | None, str | None]:
    """Return (birth_date, took_office_date) as clean date strings, or None."""
    birth_date: str | None = None
    start_date: str | None = None
    for para in h.xpath_elements(
        doc, './/div[@id="person-about"]//div[@class="text"]/p'
    ):
        text = h.element_text(para)
        if text.startswith("Дата рождения:"):
            birth_date = text.removeprefix("Дата рождения:").strip()
        elif text.startswith("Дата вступления в полномочия:"):
            start_date = text.removeprefix("Дата вступления в полномочия:").strip()

    if birth_date is None:
        # Prominent deputies (e.g. chamber leadership) carry a "Биография"
        # section instead, with the birth date as the first entry's date term.
        bio_sections = h.xpath_elements(
            doc,
            './/div[@id="person-about"]/section[h2[contains(normalize-space(.), "Биография")]]',
        )
        if bio_sections:
            dates = h.xpath_elements(bio_sections[0], ".//dl/dt")
            descriptions = h.xpath_elements(bio_sections[0], ".//dl/dd")
            for date_el, description_el in zip(dates, descriptions):
                if "Родил" in h.element_text(description_el):
                    birth_date = h.element_text(date_el)
                    break

    return birth_date, start_date


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

    details_url = f"{BASE_URL}{person_id}/"
    doc = context.fetch_html(details_url, cache_days=7)
    birth_date, start_date = extract_details(doc)
    h.apply_date(person, "birthDate", birth_date)

    # Russian citizenship is required for a State Duma seat, and holding foreign
    # citizenship or a foreign residence permit disqualifies a candidate, per
    # Article 97(1) of the Constitution of the Russian Federation:
    # https://constitutionrf.ru/rzd-1/gl-5/st-97-krf
    person.add("citizenship", "ru")
    person.add("sourceUrl", details_url)

    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
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
    data = context.fetch_json(context.data_url, cache_days=1)
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
