from zavod import Context, helpers as h

from zavod.stateful.positions import categorise

IGNORE = [
    "current_committee_memberships",
    "former_committee_memberships",
    "affiliations",
    "language_code",
    "type_code",
    "sort_name",
    "electoral_districts",
    "parliamentary_status",
    "Kansanedustajana",
    "parliamentary_groups",
    "phone",
    "email",
    "nickname",
    "education",
    "parliamentary_term_suspended",
    "work_history",
    "government_memberships",
]


def translate_keys(context, data) -> dict|list:
    """Recursively translate keys in nested dictionaries and lists."""
    if isinstance(data, dict):
        translated = {}
        for key, value in data.items():
            # Translate the key
            new_key = context.lookup_value("columns", key) or key
            # Recursively translate the value
            translated[new_key] = translate_keys(context, value)
        return translated
    elif isinstance(data, list):
        # Recursively translate each item in the list
        return [translate_keys(context, item) for item in data]
    else:
        # Base case: return primitive values as-is
        return data


def get_all_parliamentary_terms(edustajatoimet):
    """Extract all parliamentary terms as a list."""
    edustajatoimi = edustajatoimet.pop("parliamentary_term")

    if isinstance(edustajatoimi, dict):
        return [edustajatoimi]
    elif isinstance(edustajatoimi, list):
        return edustajatoimi
    else:
        return []


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url, cache_days=5)
    for item in data:
        id = item.pop("hetekaId")
        pep_data = context.fetch_json(
            f"https://avoindata.eduskunta.fi/api/v1/memberofparliament/{id}/fi",
            cache_days=5,
        )
        translated = translate_keys(context, pep_data)
        henkilo = translated.get("jsonNode", {}).get("person", {})
        first_name = henkilo.pop("first_name")
        last_name = henkilo.pop("last_name")
        birth_year = henkilo.pop("birth_year")
        person_id = henkilo.pop("person_id")

        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, birth_year)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
        entity.add(
            "sourceUrl",
            f"https://avoindata.eduskunta.fi/api/v1/memberofparliament/{person_id}/fi",
        )
        entity.add("birthPlace", henkilo.pop("birth_place", None))
        entity.add("birthDate", birth_year)
        entity.add("citizenship", "fi")
        entity.add("gender", henkilo.pop("gender"))
        entity.add("address", henkilo.pop("current_municipality"))
        entity.add("position", henkilo.pop("occupation"))

        position = h.make_position(
            context,
            name="Member of the Parliament of Finland",
            wikidata_id="Q17592486",
            country="fi",
            topics=["gov.legislative", "gov.national"],
            lang="eng",
        )

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            return

        # Get parliamentary terms
        parliamentary_terms = henkilo.pop("parliamentary_terms", {})
        all_terms = get_all_parliamentary_terms(parliamentary_terms)

        for term in all_terms:
            start_date = term.pop("start_date")
            end_date = term.pop("end_date", None)

            occupancy = h.make_occupancy(
                context,
                person=entity,
                position=position,
                start_date=start_date,
                end_date=end_date,
                categorisation=categorisation,
            )
            if occupancy:
                context.emit(entity)
                context.emit(position)
                context.emit(occupancy)

        context.audit_data(henkilo, IGNORE)
