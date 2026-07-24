from functools import lru_cache

from normality import WS
from followthemoney import registry
from rigour.names import reduce_names
from nomenklatura.resolver import Identifier, Linker

from zavod.entity import Entity


PROV_MIN_DATES = ("createdAt", "authoredAt", "publishedAt")
PROV_MAX_DATES = ("modifiedAt", "retrievedAt")


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
FULL_NAME_PROPS = {"name", "alias"}

# We never want to remove names stated by these datasets, even if they look short,
# if another dataset states that they are a weakAlias, whatever.
# Never touch OFAC names, people don't like when we do that.
NEVER_REMOVE_NAMES_DATASETS = {
    "us_ofac_sdn",
    "us_ofac_cons",
    # Has every name from us_ofac IN ALL UPPER CASE JUST TO MAKE SURE
    # so even though it's somewhat important, we don't want to make it holy for now.
    # "us_trade_csl",
    # The names suck sometimes and customer's don't have as high expectations for
    # these to be reproduced verbatim.
    # "eu_journal_sanctions",
    # "eu_fsf",
    "eu_sanctions_map",
    "gb_fcdo_sanctions",
    "ca_dfatd_sema_sanctions",
    "au_dfat_sanctions",
}


@lru_cache(maxsize=10000)
def _remove_prefix_date_values(values: tuple[str, ...]) -> tuple[str, ...]:
    """See ``_simplify_dates``."""
    kept: list[str] = []
    values_list = sorted(values, reverse=True)
    for index, value in enumerate(values_list):
        if index > 0:
            longer = values_list[index - 1]
            if longer.startswith(value):
                continue
        kept.append(value)
    return tuple(kept)


def _simplify_dates(entity: Entity) -> Entity:
    """If an entity has multiple values for a date field, you may
    want to remove all those that are prefixes of others. For example,
    if a Person has both a birthDate of 1990 and of 1990-05-01, we'd
    want to drop the mention of 1990."""
    for prop in entity.iterprops():
        if prop.type == registry.date:
            # Unrolled hot path: called for every entity in every export.
            stmts = entity._statements[prop.name]
            if len(stmts) < 2:
                continue
            values_in = tuple({s.value for s in stmts})
            if len(values_in) < 2:
                continue
            values = _remove_prefix_date_values(values_in)
            if prop.name in PROV_MAX_DATES:
                values = (max(values),)
            elif prop.name in PROV_MIN_DATES:
                values = (min(values),)

            # If the sentinel HISTORIC is present, remove it during entity
            # consolidation.
            if registry.date.HISTORIC in values:
                values = tuple(v for v in values if v != registry.date.HISTORIC)

            for stmt in list(stmts):
                if stmt.value not in values:
                    entity._statements[prop.name].remove(stmt)
    return entity


def _simplify_undirected(entity: Entity) -> Entity:
    """Simplify undirected edges by removing duplicate entity IDs on both
    ends."""
    # Problem: undirected relationships in which both
    # entities are given as the source AND the target
    if (
        not entity.schema.edge
        or entity.schema.edge_directed
        or not entity.schema.edge_source
        or not entity.schema.edge_target
    ):
        return entity
    sources = entity.get_statements(entity.schema.edge_source)
    targets = entity.get_statements(entity.schema.edge_target)
    source_ids = set(s.value for s in sources)
    target_ids = set(t.value for t in targets)
    common = source_ids.intersection(target_ids)
    if len(common) != 2:
        return entity
    identifiers = [Identifier.get(s) for s in common]
    source_id, target_id = max(identifiers), min(identifiers)
    for stmt in sources:
        if stmt.value == target_id:
            entity._statements[entity.schema.edge_source].remove(stmt)
    for stmt in targets:
        if stmt.value == source_id:
            entity._statements[entity.schema.edge_target].remove(stmt)
    return entity


def simplify_names(entity: Entity) -> Entity:
    """Simplify the names of an entity, removing variants in case and names which do not include a letter."""
    if not entity.schema.is_a("LegalEntity"):
        return entity

    # Collect weak aliases (short names which are not very reliable)
    # 15 characters is arbitrary, but should filter for "noms de guerre" but leave more detailed names
    # marked as weakAlias. Let's make this longer after we've done a bit of data remediation on the source data
    # to make sure all weakAlias are actually weak.
    # We do this to mitigate bad data from watchlists where source data format is not very expressive
    # or the data quality management is poor.
    weak_aliases = entity.get("weakAlias", quiet=True)
    weak_aliases = [a.casefold() for a in weak_aliases if len(a) < 15 or WS not in a]

    for prop_ in NAME_PROPS:
        prop = entity.schema.get(prop_)
        if prop is None:
            continue
        names = entity.get(prop)
        len_names_original = len(names)

        # Remove names which are marked at weakAlias by at least one other source.
        if prop.name in FULL_NAME_PROPS and len(weak_aliases):
            strong_names = [n for n in names if n.casefold() not in weak_aliases]
            # We only want to demote names to weakAlias if that would leave any left in the field
            # In the alias field we're okay losing them all.
            if len(strong_names) > 0 or prop.name == "alias":
                names = strong_names
        reduced = reduce_names(names)

        # As a performance optimization, we only iterate over (and potentially remove) statements
        # if the number of names has been reduced.
        if len(reduced) < len_names_original:
            stmts = list(entity._statements.get(prop_, set()))
            for stmt in stmts:
                # We never want to touch names that are marked full names in
                # the core sanctions lists.
                if stmt.dataset in NEVER_REMOVE_NAMES_DATASETS:
                    continue

                if stmt.value not in reduced:
                    entity._statements[prop_].remove(stmt)

    entity._caption = None
    # TODO: do we want to do cross-field deduplication here? We could deduplicate between
    # alias and name, as well as:
    # firstName, secondName, middleName, patronymic, matronymic
    return entity


def consolidate_entity(linker: Linker[Entity], entity: Entity) -> Entity:
    """Consolidate an entity by simplifying some of its properties."""
    if entity.id is not None:
        entity.extra_referents.update(linker.get_referents(entity.id))
    entity = _simplify_dates(entity)
    entity = simplify_names(entity)
    entity = _simplify_undirected(entity)
    return entity
