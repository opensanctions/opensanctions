from functools import lru_cache

from normality import WS
from followthemoney import Statement, registry
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
    """See ``_redundant_dates``."""
    kept: list[str] = []
    values_list = sorted(values, reverse=True)
    for index, value in enumerate(values_list):
        if index > 0:
            longer = values_list[index - 1]
            if longer.startswith(value):
                continue
        kept.append(value)
    return tuple(kept)


Removals = dict[str, set[Statement]]
"""Statements to drop from an entity, keyed by property."""


def _redundant_dates(entity: Entity) -> Removals:
    """If an entity has multiple values for a date field, you may
    want to remove all those that are prefixes of others. For example,
    if a Person has both a birthDate of 1990 and of 1990-05-01, we'd
    want to drop the mention of 1990."""
    removed: Removals = {}
    for prop_name, stmts in entity._statements.items():
        prop = entity.schema.properties.get(prop_name)
        if prop is None or prop.type != registry.date:
            continue
        # Unrolled hot path: called for every entity in every export.
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

        redundant = {s for s in stmts if s.value not in values}
        if len(redundant) > 0:
            removed[prop.name] = redundant
    return removed


def _redundant_undirected_ends(entity: Entity) -> Removals:
    """Simplify undirected edges by removing duplicate entity IDs on both
    ends."""
    schema = entity.schema
    if (
        not schema.edge
        or schema.edge_directed
        or not schema.edge_source
        or not schema.edge_target
    ):
        return {}
    sources = entity._statements.get(schema.edge_source, set())
    targets = entity._statements.get(schema.edge_target, set())
    source_ids = set(s.value for s in sources)
    target_ids = set(t.value for t in targets)
    common = source_ids.intersection(target_ids)
    if len(common) != 2:
        return {}
    identifiers = [Identifier.get(s) for s in common]
    source_id, target_id = max(identifiers), min(identifiers)
    return {
        schema.edge_source: {s for s in sources if s.value == target_id},
        schema.edge_target: {t for t in targets if t.value == source_id},
    }


def _redundant_names(entity: Entity) -> Removals:
    """Simplify the names of an entity, removing variants in case and names
    which do not include a letter."""
    if not entity.schema.is_a("LegalEntity"):
        return {}

    # Collect weak aliases (short names which are not very reliable)
    # 15 characters is arbitrary, but should filter for "noms de guerre" but leave more detailed names
    # marked as weakAlias. Let's make this longer after we've done a bit of data remediation on the source data
    # to make sure all weakAlias are actually weak.
    # We do this to mitigate bad data from watchlists where source data format is not very expressive
    # or the data quality management is poor.
    weak_aliases = entity.get("weakAlias", quiet=True)
    weak_aliases = [a.casefold() for a in weak_aliases if len(a) < 15 or WS not in a]

    removed: Removals = {}
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
            redundant = {
                s
                for s in entity._statements.get(prop_, set())
                # We never want to touch names that are marked full names in
                # the core sanctions lists.
                if s.dataset not in NEVER_REMOVE_NAMES_DATASETS
                and s.value not in reduced
            }
            if len(redundant) > 0:
                removed[prop_] = redundant

    # TODO: do we want to do cross-field deduplication here? We could deduplicate between
    # alias and name, as well as:
    # firstName, secondName, middleName, patronymic, matronymic
    return removed


def consolidate_entity(linker: Linker[Entity], entity: Entity) -> Entity:
    """Consolidate an entity by simplifying some of its properties.

    The given entity keeps its statements, as the sources stated them. When
    there is anything to simplify, the consolidated entity is a copy without
    the redundant statements; otherwise it is the given entity itself."""
    if entity.id is not None:
        entity.extra_referents.update(linker.get_referents(entity.id))
    # Each step looks at its own properties (dates, names, edge ends), so all
    # of them can work from the given entity.
    removed: Removals = {}
    for step in (_redundant_dates, _redundant_names, _redundant_undirected_ends):
        for prop, stmts in step(entity).items():
            removed.setdefault(prop, set()).update(stmts)
    if len(removed) == 0:
        return entity
    return entity.clone_without_statements(removed)
