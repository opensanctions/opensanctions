from datetime import timedelta

from opensanctions import settings
from opensanctions.core import Entity

BIRTH_CUTOFF = settings.DEATH_CUTOFF - timedelta(days=100 * 365)


def check_person_cutoff(entity: Entity):
    if not entity.schema.is_a("Person"):
        return False
    death_dates = entity.get("deathDate", quiet=True)
    # print("DEATH_DATES", death_dates)
    death_cutoff = settings.DEATH_CUTOFF.isoformat()
    if len(death_dates) and max(death_dates) < death_cutoff:
        return True
    birth_dates = entity.get("birthDate", quiet=True)
    # print("BIRTH_DATES", birth_dates)
    birth_cutoff = BIRTH_CUTOFF.isoformat()
    if len(birth_dates) and min(birth_dates) < birth_cutoff:
        return True
    return False


if __name__ == "__main__":
    from followthemoney import model

    entity = model.make_entity("Person")
    entity.add("birthDate", "1985")
    assert not check_person_cutoff(entity)

    entity = model.make_entity("Person")
    entity.add("birthDate", "1985")
    entity.add("deathDate", "2022")
    assert not check_person_cutoff(entity)

    entity = model.make_entity("Person")
    entity.add("birthDate", "1800")
    assert check_person_cutoff(entity)

    entity = model.make_entity("Person")
    entity.add("birthDate", "1985")
    entity.add("deathDate", "2008")
    assert check_person_cutoff(entity)
