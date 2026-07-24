"""Unit tests for the ann_graph_topics analyzer.

Each test builds a small in-memory store with the entities relevant to one
rule, runs ``analyze_entity`` (or a rule directly) against a ``FakeContext``
that captures emitted patches, and asserts on the set of ``(target_id, topic)``
pairs that came out.
"""

from nomenklatura.resolver import Linker
from nomenklatura.store.memory import MemoryStore
from zavod import Context, Dataset, Entity
from zavod.logs import get_logger

from .analyzer import analyze_entity, non_graph_topics

SOURCE = Dataset({"name": "src", "title": "Source"})
GRAPH = Dataset({"name": "ann_graph_topics", "title": "Graph"})


class FakeContext(Context):
    """Test double for zavod's ``Context``.

    Bypasses ``Context.__init__`` (which sets up a DB session, HTTP session
    and a file-backed statement writer) and populates only the attributes the
    analyzer's rules touch: ``dataset``, ``log``, and a captured ``emit``
    buffer. ``make`` is inherited unchanged.
    """

    def __init__(self, dataset: Dataset = GRAPH) -> None:
        self.dataset = dataset
        self.log = get_logger(dataset.name)
        self.emitted: list[tuple[Entity, bool]] = []

    def emit(
        self,
        entity: Entity,
        external: bool = False,
        origin: str | None = None,
    ) -> None:
        self.emitted.append((entity, external))


def _entity(
    schema: str,
    id: str,
    properties: dict[str, list[str]] | None = None,
    dataset: Dataset = SOURCE,
    external: bool = False,
) -> Entity:
    entity = Entity.from_data(
        dataset,
        {"schema": schema, "id": id, "properties": properties or {}},
    )
    if external:
        for stmt in entity.statements:
            stmt._external = True
    return entity


def _store(
    entities: list[Entity], scope: Dataset = SOURCE
) -> MemoryStore[Dataset, Entity]:
    linker: Linker[Entity] = Linker({})
    store: MemoryStore[Dataset, Entity] = MemoryStore(scope, linker)
    # Return zavod's Entity subclass (which carries ``external``) from view
    # lookups, matching what the analyzer sees at runtime.
    store.entity_class = Entity
    writer = store.writer()
    for entity in entities:
        writer.add_entity(entity)
    writer.flush()
    return store


def _emits(ctx: FakeContext) -> list[tuple[str, str]]:
    """Flatten captured emits to ``(target_id, topic)`` pairs."""
    out: list[tuple[str, str]] = []
    for entity, _external in ctx.emitted:
        assert entity.id is not None
        for topic in entity.get("topics"):
            out.append((entity.id, topic))
    return out


def _analyze(entities: list[Entity], source_id: str) -> FakeContext:
    store = _store(entities)
    view = store.view(SOURCE, external=True)
    source = view.get_entity(source_id)
    assert source is not None
    ctx = FakeContext()
    analyze_entity(ctx, view, source)
    return ctx


# ---- rule_pep_family_to_rca ---------------------------------------------


def test_rca_emitted_for_family_of_pep() -> None:
    ctx = _analyze(
        [
            _entity("Person", "pep", {"topics": ["role.pep"]}),
            _entity("Family", "fam", {"person": ["pep"], "relative": ["spouse"]}),
            _entity("Person", "spouse"),
        ],
        source_id="pep",
    )
    assert ("spouse", "role.rca") in _emits(ctx)


def test_rca_skipped_if_target_already_rca_or_pep() -> None:
    ctx = _analyze(
        [
            _entity("Person", "pep", {"topics": ["role.pep"]}),
            _entity("Family", "fam", {"person": ["pep"], "relative": ["spouse"]}),
            _entity("Person", "spouse", {"topics": ["role.rca"]}),
        ],
        source_id="pep",
    )
    assert _emits(ctx) == []


# ---- rule_sanction_adjacency --------------------------------------------


def test_sanction_linked_via_ownership_edge() -> None:
    ctx = _analyze(
        [
            _entity("Person", "boss", {"topics": ["sanction"]}),
            _entity("Ownership", "own", {"owner": ["boss"], "asset": ["acme"]}),
            _entity("Company", "acme"),
        ],
        source_id="boss",
    )
    assert ("acme", "sanction.linked") in _emits(ctx)


def test_sanction_linked_via_direct_securities_property() -> None:
    ctx = _analyze(
        [
            _entity("Company", "co", {"topics": ["sanction"]}),
            _entity("Security", "sec1", {"issuer": ["co"]}),
        ],
        source_id="co",
    )
    assert ("sec1", "sanction.linked") in _emits(ctx)


def test_sanction_linked_from_sanctioned_security_to_issuer() -> None:
    # A sanctioned Security tags its issuer as sanction.linked — the
    # reverse direction of the Company → Security path.
    ctx = _analyze(
        [
            _entity("Company", "co"),
            _entity("Security", "sec1", {"topics": ["sanction"], "issuer": ["co"]}),
        ],
        source_id="sec1",
    )
    assert ("co", "sanction.linked") in _emits(ctx)


def test_sanction_linked_not_emitted_via_unlisted_edge() -> None:
    # UnknownLink is not in SANCTION_ADJACENCY_EDGES; the rule must ignore it.
    ctx = _analyze(
        [
            _entity("Person", "boss", {"topics": ["sanction"]}),
            _entity(
                "UnknownLink",
                "link",
                {"subject": ["boss"], "object": ["other"]},
            ),
            _entity("Person", "other"),
        ],
        source_id="boss",
    )
    assert _emits(ctx) == []


def test_sanction_linked_skipped_if_target_already_seed() -> None:
    ctx = _analyze(
        [
            _entity("Person", "boss", {"topics": ["sanction"]}),
            _entity("Ownership", "own", {"owner": ["boss"], "asset": ["acme"]}),
            _entity("Company", "acme", {"topics": ["sanction"]}),
        ],
        source_id="boss",
    )
    assert _emits(ctx) == []


# ---- rule_ownership_descent ---------------------------------------------


def test_ownership_descent_emits_on_asset() -> None:
    # An owner already tagged sanction.linked (as if from a prior run) pushes
    # the tag one hop further to its asset.
    ctx = _analyze(
        [
            _entity("Company", "parent", {"topics": ["sanction.linked"]}),
            _entity(
                "Ownership",
                "own",
                {"owner": ["parent"], "asset": ["child"]},
            ),
            _entity("Company", "child"),
        ],
        source_id="parent",
    )
    assert ("child", "sanction.linked") in _emits(ctx)


def test_ownership_descent_does_not_ascend() -> None:
    # From the asset side, the rule must not push the tag up to the owner.
    ctx = _analyze(
        [
            _entity("Company", "parent"),
            _entity(
                "Ownership",
                "own",
                {"owner": ["parent"], "asset": ["child"]},
            ),
            _entity("Company", "child", {"topics": ["sanction.linked"]}),
        ],
        source_id="child",
    )
    assert _emits(ctx) == []


def test_ownership_descent_ignores_non_ownership_edges() -> None:
    # Directorship is out of scope for the descent rule.
    ctx = _analyze(
        [
            _entity("Person", "director", {"topics": ["sanction.linked"]}),
            _entity(
                "Directorship",
                "dir",
                {"director": ["director"], "organization": ["co"]},
            ),
            _entity("Company", "co"),
        ],
        source_id="director",
    )
    assert _emits(ctx) == []


# ---- rule_sanction_control_descent --------------------------------------


def test_sanction_control_descends_from_sanctioned_owner() -> None:
    # A directly sanctioned owner tags the asset sanction.control (with the
    # sanction.linked co-emit) on the first pass.
    ctx = _analyze(
        [
            _entity("Person", "boss", {"topics": ["sanction"]}),
            _entity(
                "Ownership",
                "own",
                {"owner": ["boss"], "asset": ["acme"]},
            ),
            _entity("Company", "acme"),
        ],
        source_id="boss",
    )
    emits = _emits(ctx)
    assert ("acme", "sanction.control") in emits
    assert ("acme", "sanction.linked") in emits


def test_sanction_control_descent_propagates_from_control_seed() -> None:
    # An entity carrying sanction.control (from a prior run) continues the
    # descent one hop further.
    ctx = _analyze(
        [
            _entity("Company", "parent", {"topics": ["sanction.control"]}),
            _entity(
                "Ownership",
                "own",
                {"owner": ["parent"], "asset": ["child"]},
            ),
            _entity("Company", "child"),
        ],
        source_id="parent",
    )
    assert ("child", "sanction.control") in _emits(ctx)
    assert ("child", "sanction.linked") in _emits(ctx)


def test_sanction_control_does_not_descend_directorship() -> None:
    ctx = _analyze(
        [
            _entity("Person", "director", {"topics": ["sanction"]}),
            _entity(
                "Directorship",
                "dir",
                {"director": ["director"], "organization": ["co"]},
            ),
            _entity("Company", "co"),
        ],
        source_id="director",
    )
    emits = _emits(ctx)
    assert ("co", "sanction.control") not in emits


def test_sanction_control_descent_does_not_ascend_ownership() -> None:
    # From the asset side, do not tag the owner.
    ctx = _analyze(
        [
            _entity("Company", "parent"),
            _entity(
                "Ownership",
                "own",
                {"owner": ["parent"], "asset": ["child"]},
            ),
            _entity("Company", "child", {"topics": ["sanction.control"]}),
        ],
        source_id="child",
    )
    assert _emits(ctx) == []


def test_sanction_control_descent_ignores_non_control_edges() -> None:
    # Membership is not part of the control chain — sanction.control must
    # not spread across it.
    ctx = _analyze(
        [
            _entity("Person", "boss", {"topics": ["sanction"]}),
            _entity(
                "Membership",
                "mem",
                {"member": ["boss"], "organization": ["club"]},
            ),
            _entity("Organization", "club"),
        ],
        source_id="boss",
    )
    topics = {topic for _id, topic in _emits(ctx)}
    assert "sanction.control" not in topics


def test_sanction_control_descent_skips_target_already_controlled() -> None:
    # If the target already carries sanction or sanction.control from another
    # source, don't re-emit either topic.
    ctx = _analyze(
        [
            _entity("Person", "boss", {"topics": ["sanction"]}),
            _entity(
                "Ownership",
                "own",
                {"owner": ["boss"], "asset": ["acme"]},
            ),
            _entity("Company", "acme", {"topics": ["sanction.control"]}),
        ],
        source_id="boss",
    )
    emits = _emits(ctx)
    assert ("acme", "sanction.control") not in emits
    # rule_sanction_adjacency still fires and tags sanction.linked on the
    # asset via the broad-edge Ownership path — but SANCTION_SEEDS check
    # in that rule requires the target to lack sanction/sanction.linked; here
    # it has sanction.control (not seeded for that rule) so it *will* emit.
    # The important assertion for THIS rule is only that sanction.control
    # wasn't re-emitted, which is above.


def test_sanction_control_descent_terminated_by_end_date() -> None:
    # Ex-director doesn't propagate — the endDate skip is shared with the
    # rest of the rules but included here as a smoke test.
    ctx = _analyze(
        [
            _entity("Person", "director", {"topics": ["sanction"]}),
            _entity(
                "Directorship",
                "dir",
                {
                    "director": ["director"],
                    "organization": ["co"],
                    "endDate": ["2020-01-01"],
                },
            ),
            _entity("Company", "co"),
        ],
        source_id="director",
    )
    topics = {topic for _id, topic in _emits(ctx)}
    assert "sanction.control" not in topics


# ---- rule_export_control_descent ----------------------------------------


def test_export_control_descent_emits_from_direct_seed() -> None:
    # A directly listed export.control entity pushes export.control.linked to
    # its immediate downstream asset on the first run.
    ctx = _analyze(
        [
            _entity("Company", "parent", {"topics": ["export.control"]}),
            _entity(
                "Ownership",
                "own",
                {"owner": ["parent"], "asset": ["child"]},
            ),
            _entity("Company", "child"),
        ],
        source_id="parent",
    )
    assert ("child", "export.control.linked") in _emits(ctx)


def test_export_control_descent_propagates_from_linked() -> None:
    # A prior-run tag (export.control.linked) advances one hop further.
    ctx = _analyze(
        [
            _entity("Company", "parent", {"topics": ["export.control.linked"]}),
            _entity(
                "Ownership",
                "own",
                {"owner": ["parent"], "asset": ["child"]},
            ),
            _entity("Company", "child"),
        ],
        source_id="parent",
    )
    assert ("child", "export.control.linked") in _emits(ctx)


def test_export_control_descent_does_not_ascend() -> None:
    # Ownership descent is downward-only; from the asset side we must not
    # tag the owner.
    ctx = _analyze(
        [
            _entity("Company", "parent"),
            _entity(
                "Ownership",
                "own",
                {"owner": ["parent"], "asset": ["child"]},
            ),
            _entity(
                "Company",
                "child",
                {"topics": ["export.control.linked"]},
            ),
        ],
        source_id="child",
    )
    assert _emits(ctx) == []


def test_export_control_descent_ignores_directorship() -> None:
    # Deliberately narrower than sanction.control: Directorship must NOT
    # propagate export.control.linked. The BIS rule is ownership-based.
    ctx = _analyze(
        [
            _entity("Person", "director", {"topics": ["export.control"]}),
            _entity(
                "Directorship",
                "dir",
                {"director": ["director"], "organization": ["co"]},
            ),
            _entity("Company", "co"),
        ],
        source_id="director",
    )
    assert _emits(ctx) == []


def test_export_control_descent_skips_target_already_seeded() -> None:
    ctx = _analyze(
        [
            _entity("Company", "parent", {"topics": ["export.control"]}),
            _entity(
                "Ownership",
                "own",
                {"owner": ["parent"], "asset": ["child"]},
            ),
            _entity(
                "Company",
                "child",
                {"topics": ["export.control"]},
            ),
        ],
        source_id="parent",
    )
    assert _emits(ctx) == []


def test_export_control_descent_does_not_coemit_sanction_linked() -> None:
    # Explicit: export.control.linked is NOT sanction.linked despite the
    # suffix, and we must not co-emit the sanctions topic. This test guards
    # against a plausible future "correction".
    ctx = _analyze(
        [
            _entity("Company", "parent", {"topics": ["export.control"]}),
            _entity(
                "Ownership",
                "own",
                {"owner": ["parent"], "asset": ["child"]},
            ),
            _entity("Company", "child"),
        ],
        source_id="parent",
    )
    topics = {topic for _id, topic in _emits(ctx)}
    assert "sanction.linked" not in topics
    assert "sanction.control" not in topics


# ---- analyze_entity plumbing --------------------------------------------


def test_end_date_terminates_propagation() -> None:
    # An ex-spouse (endDate on the Family edge) should not receive role.rca.
    ctx = _analyze(
        [
            _entity("Person", "pep", {"topics": ["role.pep"]}),
            _entity(
                "Family",
                "fam",
                {
                    "person": ["pep"],
                    "relative": ["exspouse"],
                    "endDate": ["2020-01-01"],
                },
            ),
            _entity("Person", "exspouse"),
        ],
        source_id="pep",
    )
    assert _emits(ctx) == []


def test_emit_patch_has_reduced_legalentity_schema() -> None:
    # A Company target should be patched as LegalEntity so a stale annotation
    # doesn't pin the more specific schema.
    ctx = _analyze(
        [
            _entity("Person", "boss", {"topics": ["sanction"]}),
            _entity("Ownership", "own", {"owner": ["boss"], "asset": ["acme"]}),
            _entity("Company", "acme"),
        ],
        source_id="boss",
    )
    patches = {e.id: e for e, _ in ctx.emitted}
    assert patches["acme"].schema.name == "LegalEntity"


def test_emit_patch_preserves_non_legalentity_schema() -> None:
    # Security is not a LegalEntity; the patch keeps its concrete schema.
    ctx = _analyze(
        [
            _entity("Company", "co", {"topics": ["sanction"]}),
            _entity("Security", "sec1", {"issuer": ["co"]}),
        ],
        source_id="co",
    )
    patches = {e.id: e for e, _ in ctx.emitted}
    assert patches["sec1"].schema.name == "Security"


# ---- emit_patch external-ness --------------------------------------------


def _patch_external(ctx: FakeContext, target_id: str) -> bool:
    flags = {ext for entity, ext in ctx.emitted if entity.id == target_id}
    assert len(flags) == 1, flags
    return flags.pop()


def test_patch_internal_for_published_target() -> None:
    # The spouse has internal source statements, so the derived topic is
    # published along with them.
    ctx = _analyze(
        [
            _entity("Person", "pep", {"topics": ["role.pep"]}),
            _entity("Family", "fam", {"person": ["pep"], "relative": ["spouse"]}),
            _entity("Person", "spouse", {"name": ["Jane Doe"]}),
        ],
        source_id="pep",
    )
    assert _patch_external(ctx, "spouse") is False


def test_patch_external_for_passenger_target() -> None:
    # The spouse only exists as an external enrichment passenger; tagging it
    # must not, on its own, pull it into the published export.
    ctx = _analyze(
        [
            _entity("Person", "pep", {"topics": ["role.pep"]}),
            _entity("Family", "fam", {"person": ["pep"], "relative": ["spouse"]}),
            _entity("Person", "spouse", {"name": ["Jane Doe"]}, external=True),
        ],
        source_id="pep",
    )
    assert _patch_external(ctx, "spouse") is True


def test_patch_external_despite_prior_own_patch() -> None:
    # A previously emitted internal patch must not keep the target internal
    # once its source data has been demoted to external: analyzer statements
    # are discounted when judging published substance.
    ctx = _analyze(
        [
            _entity("Person", "pep", {"topics": ["role.pep"]}),
            _entity("Family", "fam", {"person": ["pep"], "relative": ["spouse"]}),
            _entity("Person", "spouse", {"name": ["Jane Doe"]}, external=True),
            _entity("Person", "spouse", {"topics": ["role.rca"]}, dataset=GRAPH),
        ],
        source_id="pep",
    )
    assert _patch_external(ctx, "spouse") is True


# ---- non_graph_topics ---------------------------------------------------


def test_non_graph_topics_filters_out_own_dataset() -> None:
    # An entity whose only topic came from ann_graph_topics itself must appear
    # as untagged from the analyzer's perspective; only topics contributed by
    # other datasets count towards "already tagged".
    store = _store(
        [
            _entity("Person", "e", {"topics": ["poi"]}, dataset=SOURCE),
            _entity("Person", "e", {"topics": ["debarred"]}, dataset=GRAPH),
        ],
        scope=SOURCE,
    )
    view = store.view(SOURCE, external=True)
    entity = view.get_entity("e")
    assert entity is not None
    ctx = FakeContext()
    assert non_graph_topics(ctx, entity) == {"poi"}
