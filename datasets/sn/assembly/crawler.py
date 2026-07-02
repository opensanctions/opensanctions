from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The roster is a Nuxt "devalue" payload: a flat pool where container values are
# integer indices back into the same pool. Resolving one key yields the deputies.
DEPUTIES_KEY = "deputies"


def resolve(pool: list[Any], index: Any, seen: frozenset[int] = frozenset()) -> Any:
    """Dereference a Nuxt devalue index into its concrete value."""
    if not isinstance(index, int) or not 0 <= index < len(pool) or index in seen:
        return None if isinstance(index, int) else index
    value = pool[index]
    if isinstance(value, dict):
        return {k: resolve(pool, v, seen | {index}) for k, v in value.items()}
    if isinstance(value, list):
        # devalue wraps reactive values as ["Reactive", index] etc.
        if len(value) == 2 and value[0] in (
            "Reactive",
            "ShallowReactive",
            "Ref",
            "ShallowRef",
        ):
            return resolve(pool, value[1], seen | {index})
        return [resolve(pool, v, seen | {index}) for v in value]
    return value


def crawl_deputy(
    context: Context,
    deputy: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug("deputy", str(deputy["id"]))
    h.apply_name(
        person,
        first_name=deputy.get("first_name"),
        last_name=deputy.get("last_name"),
        lang="fra",
    )
    person.add("gender", deputy.get("gender"))
    h.apply_date(person, "birthDate", deputy.get("birthdate"))
    person.add("birthPlace", deputy.get("birthplace"))
    person.add("notes", deputy.get("biography"))
    # Suffrage is reserved to Senegalese nationals (Constitution art. 3).
    # https://www.constituteproject.org/constitution/Senegal_2016
    person.add("citizenship", "sn")

    electoral_list = deputy.get("electoral_list") or {}
    coalition = electoral_list.get("coalition") or {}
    person.add("political", coalition.get("name"))

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    constituency = electoral_list.get("constituency") or {}
    occupancy.add("constituency", constituency.get("name"))
    group = deputy.get("group") or {}
    occupancy.add("politicalGroup", group.get("name"))

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    pool = context.fetch_json(context.data_url, cache_days=1)
    root = resolve(pool, 2)
    if not isinstance(root, dict):
        raise ValueError("Unexpected payload root")
    # The deputies live under a query-parameterised key; match it by substring.
    keys = [k for k in root if DEPUTIES_KEY in k and isinstance(root[k], dict)]
    if len(keys) != 1:
        raise ValueError(f"Expected one deputies key, found {keys}")
    deputies = root[keys[0]].get("items")
    if not isinstance(deputies, list) or len(deputies) == 0:
        raise ValueError("No deputies resolved from payload")

    position = h.make_position(
        context,
        name="Member of the National Assembly of Senegal",
        country="sn",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q20757571",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for deputy in deputies:
        crawl_deputy(context, deputy, position, categorisation)
