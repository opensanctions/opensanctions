from normality import WS
from rigour.names import reduce_names
from nomenklatura.resolver import Linker
from nomenklatura.publish.dates import simplify_dates
from nomenklatura.publish.edges import simplify_undirected

from zavod.entity import Entity


NAME_PROPS = (
    "name",
    "alias",
    "weakAlias",
    "firstName",
    "lastName",
    "secondName",
    "middleName",
    "patronymic",
    "matronymic",
)


def simplify_names(entity: Entity) -> Entity:
    """Simplify the names of an entity, removing variants in case and names which do not include a letter."""
    if not entity.schema.is_a("LegalEntity"):
        return entity

    # Collect weak aliases (short names which are not very reliable)
    # 15 characters is arbitrary, but should filter for "noms de guerre" but leave more detailed names
    # marked as weakAlias. Let's make this longer after we've done a bit of data remediation on the source data
    # to make sure all weakAlias are actually weak.
    weak_aliases = entity.get("weakAlias", quiet=True)
    weak_aliases = [a.casefold() for a in weak_aliases if len(a) < 15 or WS not in a]

    for prop_ in NAME_PROPS:
        prop = entity.schema.get(prop_)
        if prop is None:
            continue
        names = entity.get(prop)

        # Remove names which are marked at weakAlias by at least one other source
        if prop.name != "weakAlias" and len(weak_aliases):
            strong_names = [n for n in names if n.casefold() not in weak_aliases]
            if len(strong_names) > 0:
                names = strong_names

        reduced = reduce_names(names)
        if len(reduced) < len(names):
            stmts = list(entity._statements.get(prop_, set()))
            for stmt in stmts:
                if stmt.value not in reduced:
                    entity._statements[prop_].remove(stmt)
    # TODO: do we want to do cross-field deduplication here? We could deduplicate between
    # alias and name, as well as:
    # firstName, secondName, middleName, patronymic, matronymic
    return entity


def consolidate_entity(linker: Linker[Entity], entity: Entity) -> Entity:
    """Consolidate an entity by simplifying some of its properties."""
    if entity.id is not None:
        entity.extra_referents.update(linker.get_referents(entity.id))
    entity = simplify_dates(entity)
    entity = simplify_names(entity)
    entity = simplify_undirected(entity)
    return entity
