"""Unit tests for the ann_graph_topics analyzer.

Each test builds a small in-memory store with the entities relevant to one
rule, runs ``analyze_entity`` (or a rule directly) against a ``FakeContext``
that captures emitted patches, and asserts on the set of ``(target_id, topic)``
pairs that came out.
"""

from typing import Dict, List, Optional, Tuple

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
        self.emitted: List[Tuple[Entity, bool]] = []

    def emit(
        self,
        entity: Entity,
        external: bool = False,
        origin: Optional[str] = None,
    ) -> None:
        self.emitted.append((entity, external))


def _entity(
    schema: str,
    id: str,
    properties: Optional[Dict[str, List[str]]] = None,
    dataset: Dataset = SOURCE,
) -> Entity:
    return Entity.from_data(
        dataset,
        {"schema": schema, "id": id, "properties": properties or {}},
    )


def _store(
    entities: List[Entity], scope: Dataset = SOURCE
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


def _emits(ctx: FakeContext) -> List[Tuple[str, str]]:
    """Flatten captured emits to ``(target_id, topic)`` pairs."""
    out: List[Tuple[str, str]] = []
    for entity, _external in ctx.emitted:
        assert entity.id is not None
        for topic in entity.get("topics"):
            out.append((entity.id, topic))
    return out


def _analyze(entities: List[Entity], source_id: str) -> FakeContext:
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
