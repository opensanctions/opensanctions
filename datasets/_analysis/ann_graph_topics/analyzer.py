"""Propagate risk topics across the entity graph by relationship adjacency.

Sanctions lists and PEP registers flag individual entities, but risk rarely
stops at the named entity: the spouse of a PEP, a subsidiary of a sanctioned
company, or a vessel owned through several ownership tiers all carry derived
risk. This analyzer walks the resolved graph and emits topic patches that
capture that derived risk so the affected entities become visible to screening
(and eligible for further enrichment). It implements the "risk propagation"
step of the enrichment pipeline.

Three propagation rules are applied per entity:

- ``role.rca`` — a Person reachable via a ``Family`` edge from a ``role.pep``
  is tagged as a relative or close associate.
- ``sanction.linked`` — an entity adjacent to a ``sanction`` entity through a
  curated set of edge schemata (Ownership, Directorship, Membership,
  Employment, Associate, Family, Succession), plus Securities issued by a
  sanctioned entity, is tagged as sanction-linked.
- ``sanction.linked`` along ownership chains — an asset owned by an already
  ``sanction.linked`` owner is itself tagged, pushing the tag one ownership hop
  further on each run.

Requirements and invariants that make this correct:

- **Self-exclusion.** ``non_graph_topics`` ignores topic statements contributed
  by this dataset itself, so a tag this analyzer emits does not, on its own,
  re-trigger the rules that produced it. The one deliberate exception is the
  ownership-chain rule, which reads prior ``sanction.linked`` values from the
  store in order to walk one hop at a time.
- **Iterative convergence.** Because ownership propagation advances a single hop
  per run, a multi-tier corporate hierarchy only materializes over successive
  runs. The dataset must be re-run for the graph to converge; a single pass is
  not sufficient.
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
  published, while a topic on a purely-external passenger stays external. Either
  way the topic is visible in the ``external=True`` view and continues to feed
  the next ownership hop — but tagging a passenger does not, on its own, force
  it into the published export.
- **Edge end dates terminate propagation.** Relationships carrying an
  ``endDate`` are skipped: a former director or ex-spouse does not propagate
  risk.
- **Output is a patch dataset.** Each rule emits a minimal patch carrying only
  the new topic (reduced to ``LegalEntity`` for legal-entity subtypes so stale
  annotations don't pin a more specific schema); these are merged into the
  target entities downstream rather than replacing them.
"""

from typing import Set

from followthemoney import registry

from zavod import Context, Entity
from zavod.meta import get_multi_dataset
from zavod.constants import ORIGIN_INFERRED
from zavod.store import get_store, View
from zavod.integration import get_dataset_linker


def non_graph_topics(context: Context, entity: Entity) -> Set[str]:
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
        schema = "LegalEntity"
    else:
        schema = related_entity.schema.name
    patch = context.make(schema)
    patch.id = related_entity.id
    patch.add("topics", topic, origin=ORIGIN_INFERRED)
    context.emit(patch, external=related_entity.external)


def analyze_entity(context: Context, view: View, entity: Entity) -> None:
    topics = entity.get_type_values(registry.topic)
    for prop, adjacent in view.get_adjacent(entity):
        asch = adjacent.schema

        # For when the other entity is on the other side of an edge
        other_prop = (
            asch.source_prop if prop.reverse == asch.target_prop else asch.target_prop
        )

        if len(adjacent.get("endDate", quiet=True)) > 0:
            context.log.info(
                "Skipping entity with end date",
                adjacent=adjacent.id,
                entity=entity.id,
                end=adjacent.get("endDate"),
            )
            continue

        # Tag role.rca for family relations of PEPs
        if "role.pep" in topics and adjacent.schema.is_a("Family"):
            assert other_prop is not None
            for other_id in adjacent.get(other_prop):
                other = view.get_entity(other_id)
                if other is None or not other.schema.is_a("Person"):
                    continue
                other_topics = non_graph_topics(context, other)
                if other_topics.intersection({"role.rca", "role.pep"}):
                    continue
                emit_patch(context, entity, other, "role.rca", other_topics)

        # Tag sanction.linked for sanction-linked entities
        if "sanction" in topics:
            if entity.schema.is_a("Security"):
                continue
            if prop.name == "securities" and adjacent.schema.is_a("Security"):
                adj_topics = non_graph_topics(context, adjacent)
                if adj_topics.intersection({"sanction", "sanction.linked"}):
                    continue
                emit_patch(context, entity, adjacent, "sanction.linked", adj_topics)
            if not asch.edge:
                continue
            if adjacent.schema.name not in (
                "Ownership",
                "Directorship",
                "Membership",
                "Employment",
                "Associate",
                "Family",
                "Succession",
            ):
                continue
            assert other_prop is not None
            for other_id in adjacent.get(other_prop):
                other = view.get_entity(other_id)
                if other is None:
                    continue
                other_topics = non_graph_topics(context, other)
                if other_topics.intersection({"sanction", "sanction.linked"}):
                    continue
                emit_patch(context, entity, other, "sanction.linked", other_topics)

        # Tag sanction.linked for Assets of sanction.linked Owners
        #
        # This works by each run of this analyzer tagging further along the
        # ownership chain. So sanction.linked values from this analyzer
        # may be used in subsequent runs.
        if (
            "sanction.linked" in topics
            and adjacent.schema.is_a("Ownership")
            and prop.reverse is not None
            and prop.reverse.name == "owner"
        ):
            assert other_prop is not None
            for other_id in adjacent.get(other_prop):
                other = view.get_entity(other_id)
                if other is None:
                    continue
                other_topics = non_graph_topics(context, other)
                if other_topics.intersection({"sanction", "sanction.linked"}):
                    continue
                emit_patch(context, entity, other, "sanction.linked", other_topics)


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
