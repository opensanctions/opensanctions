"""Propagate risk topics across the entity graph by relationship adjacency.

Sanctions lists and PEP registers flag individual entities, but risk rarely
stops at the named entity: the spouse of a PEP, a subsidiary of a sanctioned
company, or a vessel owned through several ownership tiers all carry derived
risk. This analyzer walks the resolved graph and emits topic patches that
capture that derived risk so the affected entities become visible to screening
(and eligible for further enrichment). It implements the "risk propagation"
step of the enrichment pipeline.

Propagation rules are applied per (entity, adjacent) pair:

- ``rule_pep_family_to_rca`` — a Person reachable via a ``Family`` edge from a
  ``role.pep`` is tagged as a relative or close associate (``role.rca``).
- ``rule_sanction_adjacency`` — an entity adjacent to a ``sanction`` entity
  through a curated set of edge schemata (Ownership, Directorship, Membership,
  Employment, Associate, Family, Succession), plus Securities issued by a
  sanctioned entity, is tagged ``sanction.linked``. Non-transitive: walks
  exactly one broad-adjacency hop from a directly sanctioned entity.
- ``rule_sanction_control_descent`` — an asset or organization controlled by a
  ``sanction`` or ``sanction.control`` entity (via ``Ownership`` owner→asset or
  ``Directorship`` director→organization) is tagged ``sanction.control`` and
  co-emitted ``sanction.linked`` (so ``sanction.linked`` is a superset of
  ``sanction.control``). Models the 50%-rule "ownership or control" reading;
  one hop per run, converging across successive runs.
- ``rule_export_control_descent`` — an asset owned by an ``export.control`` or
  ``export.control.linked`` entity is itself tagged ``export.control.linked``,
  the export-control analogue of the BIS Affiliates Rule / 50% ownership
  restriction. Ownership-only, downward-only, one hop per run.

Requirements and invariants that make this correct:

- **Self-exclusion.** ``non_graph_topics`` ignores topic statements contributed
  by this dataset itself, so a tag this analyzer emits does not, on its own,
  re-trigger the rules that produced it. The deliberate exceptions are the
  descent rules (``rule_sanction_control_descent`` and
  ``rule_export_control_descent``), which read their emitted topics back from
  the store in order to walk one hop at a time.
- **``sanction.linked`` is non-transitive.** It means *directly adjacent to
  a 'sanction' tagged entity* (via broad edge or the direct Company↔Security relation),
  plus every entity in a ``sanction.control`` chain.
- **Iterative convergence.** Because ownership propagation advances a single
  hop per run, a multi-tier corporate hierarchy only materializes over
  successive runs. The dataset must be re-run for the graph to converge; a
  single pass is not sufficient.
- **External vs. internal, and why the view is ``external=True``.** A statement
  is "external" when it is excluded from the published ``default`` exports.
  Enrichers emit adjacency "passengers" — entities pulled in only because they
  sit next to a match, carrying no risk topic of their own — as external, so
  they drop out of ``default`` rather than bloating it (without this, untagged
  enrichment passengers are roughly a quarter of all matchable entities). This
  analyzer reads the store with ``external=True`` precisely so it can *see*
  those passengers and apply the rules to them; with an internal-only view it
  would be blind to exactly the entities it needs to evaluate.
- **Patches inherit the related entity's external-ness.** A patch is emitted
  internal iff the related entity already has at least one internal statement
  (``Entity.external`` is true only when *every* statement is external),
  otherwise external. So a derived topic on a genuinely published entity is
  published, while a topic on a purely-external passenger stays external.
  Either way the topic is visible in the ``external=True`` view and continues
  to feed the next ownership hop — but tagging a passenger does not, on its
  own, force it into the published export.
- **Edge end dates terminate propagation.** Relationships carrying an
  ``endDate`` are skipped: a former director or ex-spouse does not propagate
  risk. Checked once in ``analyze_entity`` before rule dispatch.
- **Output is a patch dataset.** Each rule emits a minimal patch carrying only
  the new topic (reduced to ``LegalEntity`` for legal-entity subtypes so stale
  annotations don't pin a more specific schema); these are merged into the
  target entities downstream rather than replacing them.
"""

from typing import Iterator, Set, Tuple

from followthemoney import registry
from followthemoney.property import Property
from nomenklatura.store.base import View as BaseView

from zavod import Context, Entity
from zavod.meta import Dataset, get_multi_dataset
from zavod.constants import ORIGIN_INFERRED
from zavod.store import get_store
from zavod.integration import get_dataset_linker

# The analyzer only uses base ``View`` semantics (``get_entity``,
# ``get_adjacent``, ``entities``); typing against the base class here means
# the rules accept any store's view — including the in-memory view used by
# unit tests, without a cast.
View = BaseView[Dataset, Entity]


# Edge schemata that count as "broad adjacency" for sanction propagation.
SANCTION_ADJACENCY_EDGES = frozenset(
    {
        "Ownership",
        "Directorship",
        "Membership",
        "Employment",
        "Associate",
        "Family",
        "Succession",
    }
)

# Topics that mean "already sanction-linked" — used to skip re-tagging.
SANCTION_SEEDS = frozenset({"sanction", "sanction.linked"})

# Topics that mean "already sanction-controlled" — both seed the descent and
# suppress redundant re-tagging on downstream assets.
SANCTION_CONTROL_SEEDS = frozenset({"sanction", "sanction.control"})

# Topics that mean "already export-controlled" — both seed the descent and
# suppress redundant re-tagging on downstream assets.
EXPORT_CONTROL_SEEDS = frozenset({"export.control", "export.control.linked"})


def non_graph_topics(context: Context, entity: Entity) -> Set[str]:
    """Return topics on ``entity`` that were contributed by other datasets.

    Used to decide whether a candidate target is *already* tagged without
    observing this analyzer's own prior emits — see the ``Self-exclusion``
    invariant in the module docstring.
    """
    topic_stmts = entity.get_statements("topics")
    return {s.value for s in topic_stmts if s.dataset != context.dataset.name}


def emit_patch(
    context: Context,
    risk_source: Entity,
    related_entity: Entity,
    topic: str,
    existing_topics: Set[str],
) -> None:
    context.log.info(
        f"Adding topic: {topic}",
        risk_source=risk_source.caption,
        risk_source_id=risk_source.id,
        related_entity=related_entity.caption,
        related_entity_id=related_entity.id,
        existing_topics=list(existing_topics),
    )
    if related_entity.schema.is_a("LegalEntity"):
        schema_name = "LegalEntity"
    else:
        schema_name = related_entity.schema.name
    patch = context.make(schema_name)
    patch.id = related_entity.id
    patch.add("topics", topic, origin=ORIGIN_INFERRED)
    context.emit(patch, external=related_entity.external)


def walk_edge(
    view: View, edge: Entity, prop: Property
) -> Iterator[Tuple[Entity, Property]]:
    """Yield ``(other_end, counterpart_prop)`` pairs across an edge entity.

    ``prop`` is the property on the *source* entity that reached ``edge``. The
    counterpart is the property on the edge that points at the other node.
    """
    edge_schema = edge.schema
    if edge_schema.source_prop is None or edge_schema.target_prop is None:
        return
    if prop.reverse == edge_schema.target_prop:
        counterpart = edge_schema.source_prop
    else:
        counterpart = edge_schema.target_prop
    for other_id in edge.get(counterpart):
        other = view.get_entity(other_id)
        if other is not None:
            yield other, counterpart


# ---- Rules ---------------------------------------------------------------


def rule_pep_family_to_rca(
    context: Context,
    view: View,
    source: Entity,
    source_topics: Set[str],
    prop: Property,
    adjacent: Entity,
) -> None:
    """Tag Persons on the other side of a ``Family`` edge from a PEP."""
    if "role.pep" not in source_topics:
        return
    if not adjacent.schema.is_a("Family"):
        return
    for target, _ in walk_edge(view, adjacent, prop):
        # This is a guard for potential future schema changes. There's no valid form
        # of Family where an adjacent entity isn't a Person at the moment.
        if not target.schema.is_a("Person"):
            continue
        target_topics = non_graph_topics(context, target)
        if target_topics & {"role.rca", "role.pep"}:
            continue
        emit_patch(context, source, target, "role.rca", target_topics)


def rule_sanction_adjacency(
    context: Context,
    view: View,
    source: Entity,
    source_topics: Set[str],
    prop: Property,
    adjacent: Entity,
) -> None:
    """Tag ``sanction.linked`` on direct neighbors of a sanctioned entity.

    Two topologies:

    - Company → Security via the direct ``securities`` property (no
      intermediate edge entity).
    - Curated broad edge schemata (``SANCTION_ADJACENCY_EDGES``) walked to the
      counterpart node.
    """
    if "sanction" not in source_topics:
        return
    # A sanctioned Security itself does not propagate — sanctions on a security
    # don't inherently taint the whole issuer graph.
    if source.schema.is_a("Security"):
        return
    # Direct Company → Security relation. The adjacent entity *is* the target.
    if prop.name == "securities" and adjacent.schema.is_a("Security"):
        target_topics = non_graph_topics(context, adjacent)
        if not target_topics & SANCTION_SEEDS:
            emit_patch(context, source, adjacent, "sanction.linked", target_topics)
        return
    # Otherwise the adjacent is an edge entity; walk it to the counterpart.
    if not adjacent.schema.edge:
        return
    if adjacent.schema.name not in SANCTION_ADJACENCY_EDGES:
        return
    for target, _ in walk_edge(view, adjacent, prop):
        target_topics = non_graph_topics(context, target)
        if target_topics & SANCTION_SEEDS:
            continue
        emit_patch(context, source, target, "sanction.linked", target_topics)


def rule_sanction_control_descent(
    context: Context,
    view: View,
    source: Entity,
    source_topics: Set[str],
    prop: Property,
    adjacent: Entity,
) -> None:
    """Descend one control hop from a ``sanction`` or ``sanction.control`` seed.

    Walks one step downward along any of:

    - ``Ownership``: ``owner → asset``
    - ``Directorship``: ``director → organization``

    NOTE on ``Directorship``: this rule deliberately treats a board seat as
    part of the control chain even though a single directorship isn't
    legally "control" on its own. In practice a sanctioned director on a
    company board creates sanctions exposure for the whole corporate
    hierarchy, and we prefer to over-reach here rather than miss it.
    """
    if source_topics.isdisjoint(SANCTION_CONTROL_SEEDS):
        return
    if prop.reverse is None:
        return
    descent_side = (adjacent.schema.name, prop.reverse.name)
    if descent_side not in (("Ownership", "owner"), ("Directorship", "director")):
        return
    for target, _ in walk_edge(view, adjacent, prop):
        target_topics = non_graph_topics(context, target)
        if target_topics & SANCTION_CONTROL_SEEDS:
            continue
        emit_patch(context, source, target, "sanction.control", target_topics)
        if target_topics & SANCTION_SEEDS:
            continue
        # Anything that's under sanctioned control is also sanction-linked.
        emit_patch(context, source, target, "sanction.linked", target_topics)


def rule_export_control_descent(
    context: Context,
    view: View,
    source: Entity,
    source_topics: Set[str],
    prop: Property,
    adjacent: Entity,
) -> None:
    """Descend one ``Ownership`` hop and tag ``export.control.linked``.

    Ownership-only, downward-only (owner → asset), and self-observing so that
    the tag advances one hop per run and converges across successive runs —
    the ownership-only sibling of ``rule_sanction_control_descent``.

    NOTE on the asymmetric naming: ``export.control.linked`` carries the
    ownership-*descent* semantics — it is the export-control analogue of
    ``sanction.control``, NOT of ``sanction.linked``, despite the ``.linked``
    suffix. Export control has only one derived topic (there is no
    ``export.control.control``, and no broad-adjacency topic is needed
    because the BIS Affiliates Rule is purely ownership-based), so the single
    ``.linked`` topic takes on the control meaning. Do not later "correct"
    this rule by adding ``Directorship`` edges, an upward walk, or a
    broad-adjacency step, and do not co-emit anything.
    """
    if source_topics.isdisjoint(EXPORT_CONTROL_SEEDS):
        return
    if not adjacent.schema.is_a("Ownership"):
        return
    if prop.reverse is None or prop.reverse.name != "owner":
        return
    for target, _ in walk_edge(view, adjacent, prop):
        target_topics = non_graph_topics(context, target)
        if target_topics & EXPORT_CONTROL_SEEDS:
            continue
        emit_patch(context, source, target, "export.control.linked", target_topics)


RULES = (
    rule_pep_family_to_rca,
    rule_sanction_adjacency,
    rule_sanction_control_descent,
    rule_export_control_descent,
)


def analyze_entity(context: Context, view: View, entity: Entity) -> None:
    source_topics: Set[str] = set(entity.get_type_values(registry.topic))
    for prop, adjacent in view.get_adjacent(entity):
        if len(adjacent.get("endDate", quiet=True)) > 0:
            context.log.info(
                "Skipping entity with end date",
                adjacent=adjacent.id,
                entity=entity.id,
                end=adjacent.get("endDate"),
            )
            continue
        for rule in RULES:
            rule(context, view, entity, source_topics, prop, adjacent)


def crawl(context: Context) -> None:
    scope = get_multi_dataset(context.dataset.inputs)
    linker = get_dataset_linker(scope)
    store = get_store(scope, linker)
    store.sync()
    view = store.view(scope, external=True)

    for entity_idx, entity in enumerate(view.entities()):
        if entity_idx > 0 and entity_idx % 1000 == 0:
            context.log.info("Processed %s entities" % entity_idx)
        analyze_entity(context, view, entity)
