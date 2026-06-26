---
description: Plan for redesigning edge deduplication around endpoint/schema buckets, temporal compatibility, and protected-property conflicts.
date: 2026-06-26
tags: [edge-dedupe, deduplication, occupancy, pep]
---

# Edge dedupe plan

## Context

[#3710](https://github.com/opensanctions/opensanctions/issues/3710) asks for more aggressive edge deduplication:

- merge edges where one has dates and the other does not, if there are no opposing featured/protected properties;
- merge edges where both have dates but one date is a less precise version of the other, such as `2025` and `2025-10-01`;
- avoid treating provenance/noise properties such as `sourceUrl` and `recordId` as conflicts.

[#4134](https://github.com/opensanctions/opensanctions/issues/4134) shows a persisted over-merge:
`NK-JRLS8Fktty57ge5Kr4soZY` merged five `be_chamber` `Occupancy` fragments for Charles Michel as member of the Belgian Chamber, producing one edge with multiple `periodStart` and `periodEnd` values: `2003-2007`, `2007-2010`, `2010-2014`, `2014-2019`, and `2019-2024`.

That case is likely historical rather than a current clean-state dedupe bug. The merged edge has `first_seen` / `last_change` on 2026-04-27. `Occupancy:periodStart` and `Occupancy:periodEnd` were added to `Occupancy.temporalExtent` later, in followthemoney commit `6d0b0d6f` on 2026-05-15. Before that change, occupancies containing only `periodStart`/`periodEnd` looked temporally blank to edge-dedupe. Current clean-state dedupe should see those period values through `entity.temporal_start/end`, but resolver decisions are durable and will not split themselves.

## Goal

Replace the current exact-key plus common-start special case with a clearer pipeline:

```text
edge stream
  -> endpoint + schema buckets
  -> unambiguous temporal candidate groups
  -> schema-specific protected-property decider
  -> positive resolver decisions
```

Each positive decision should mean: same directed or undirected graph edge, same schema policy, compatible time evidence, and no protected-property conflict.

## Candidate Buckets

Collect only edge entities that have exactly one source endpoint and exactly one target endpoint. Discard edges with missing endpoints or more than one source/target entity.

Bucket by:

- schema policy, exact schema by default;
- normalized source endpoint;
- normalized target endpoint.

For directed edge schemata, preserve source/target direction. For undirected edge schemata, canonicalize endpoint order.

Do not bucket by endpoints alone. An `Ownership`, `Directorship`, `Employment`, and `UnknownLink` between the same two entities must not share a candidate pool unless a future rule explicitly defines a schema-equivalence class.

## Temporal Compatibility

Use schema-listed temporal properties to build start and end value sets. Compare values, not property names.

For each edge:

```text
temporal_starts = all values from schema.temporal_start_props
temporal_ends   = all values from schema.temporal_end_props
```

For `Occupancy`, this means `startDate`, `periodStart`, `electionDate`, `declarationDate`, and `date` are comparable as start evidence; `endDate` and `periodEnd` are comparable as end evidence. This is intentional: messy sources often use the more specific and less specific date fields inconsistently.

Parse partial ISO dates as ranges:

```text
2025       => [2025-01-01, 2025-12-31]
2025-10    => [2025-10-01, 2025-10-31]
2025-10-05 => [2025-10-05, 2025-10-05]
```

Two temporal value sets are compatible when:

- either side is empty; or
- at least one range from the left set overlaps at least one range from the right set.

Examples:

```text
compatible([], ["2025"]) == true
compatible(["2025"], ["2025-10-05"]) == true
compatible(["2025-10"], ["2025-10-05"]) == true
compatible(["2025-09"], ["2025-10-05"]) == false
compatible(["2024"], ["2025-10-05"]) == false
```

Compare start sets only to start sets, and end sets only to end sets. Do not compare start evidence to end evidence. A pair of edges is temporally compatible only if both start evidence and end evidence are compatible.

### Temporal Candidate Groups

Temporal merging is an operation on a bucket's candidate set:

1. If every edge in the candidate set is pairwise temporally compatible, pass the whole set to the protected-property decider.
2. If only a subset is pairwise temporally compatible, pass that subset to the protected-property decider.
3. If an edge could be merged into two or more mutually incompatible subsets based on temporal data, treat that edge as ambiguous and skip all temporal merges involving it.

Do not form merge groups with plain connected components, because temporal compatibility is not transitive:

```text
A: 2025
B: 2025-01-01
C: 2025-12-31
```

`A` is compatible with both `B` and `C`, but `B` and `C` conflict. Merging `A` into either one would be arbitrary, so neither merge should happen.

The temporal layer only emits unambiguous candidate groups. It does not make resolver decisions.

## Protected Properties

Protected properties are schema-specific values that can distinguish two otherwise similar edge facts. The protected-property decider runs after temporal grouping. If protected properties conflict inside a temporal candidate group, that group must not be merged.

Normalize protected values before comparison with slug-style normalization: asciify, lowercase, strip/collapse punctuation and whitespace. `normality.slugify` is the intended default unless a property later needs a type-specific comparator.

Global protected-value rules:

- `[]` matches everything.
- One shared normalized value is enough to make two non-empty value sets compatible.
- Two non-empty normalized value sets with no shared value conflict.

Examples:

```text
compatible([], ["director"]) == true
compatible(["director"], ["director"]) == true
compatible(["director", "shareholder"], ["director"]) == true
compatible(["director"], ["signatory"]) == false
compatible(["Président-directeur général"], ["president directeur general"]) == true
```

Start with explicit schema policies rather than deriving this automatically from all featured properties:

```python
PROTECTED_PROPS = {
    "Ownership": [
        "percentage",
        "sharesCount",
        "sharesValue",
        "sharesCurrency",
    ],
    "Directorship": [
        "role",
    ],
    "Employment": [
        "role",
    ],
    "Membership": [
        "role",
    ],
    "Representation": [
        "role",
    ],
    "Associate": [
        "relationship",
    ],
    "Family": [
        "relationship",
    ],
    "UnknownLink": [
        "role",
    ],
    "Value": [
        "amount",
        "currency",
        "amountUsd",
    ],
    "Occupancy": [
        "constituency",
        "politicalGroup",
        "status",
    ],
}
```

The initial policy should be explicit and test-driven. If a schema has no protected-property entry, the decider should treat it as having no protected conflicts rather than inventing rules from `featured` properties. Properties not listed in `PROTECTED_PROPS` are ignored by omission, so there is no separate ignored-property policy.

## Resolver Decisions

Only groups that pass both temporal compatibility and protected-property checks become positive resolver decisions with `user="edge-dedupe"`.

The resolver is binary, so intermediate evidence such as "date precision" or "missing date" should not be encoded as resolver state. Keep such details in logs if useful, but do not make them part of the dedupe data model unless there is a concrete operational need.

## Implementation Sketch

Keep the public command behavior: `zavod integration edges.dedupe_edges(resolver, view)` should still produce resolver decisions and commit through the existing CLI.

Refactor internals around small functions:

```python
def get_vertices(entity: Entity) -> tuple[Identifier, Identifier] | None:
    ...

def bucket_key(entity: Entity) -> BucketKey | None:
    ...

def temporal_values(entity: Entity) -> TemporalValues:
    ...

def dates_compatible(left: list[str], right: list[str]) -> bool:
    ...

def pair_temporally_compatible(left: Entity, right: Entity) -> bool:
    ...

def temporal_candidate_groups(entities: list[Entity]) -> list[list[Entity]]:
    ...

def protected_values(entity: Entity, prop: str) -> set[str]:
    ...

def props_compatible(entities: list[Entity]) -> bool:
    ...
```

The grouping function should return entity IDs or entities grouped as final resolver-decision candidates. The merge function can remain close to the current `merge_groups()` behavior.

Avoid a large generic matcher. The deduper should stay explainable because every automatic merge writes durable resolver judgements.

## Tests

Add focused tests in `zavod/zavod/tests/integration/test_edges.py`:

- buckets preserve direction for directed edges;
- buckets canonicalize endpoints for undirected edges;
- edges with multiple sources or targets are skipped;
- different schemata with the same endpoints do not merge;
- exact same temporal extent merges;
- missing dates are temporally compatible with dated edges;
- partial dates overlap: `2025` merges with `2025-10-01`;
- incompatible dates do not merge: `2025-09` does not merge with `2025-10-01`;
- ambiguity is skipped: `2025` does not merge into either `2025-01-01` or `2025-12-31`;
- protected props with a shared normalized value are compatible;
- protected props with non-empty disjoint normalized values block merging;
- empty protected props do not block merging;
- unprotected provenance/noise props do not block merging.

Add a regression fixture shaped like #4134:

- same `Occupancy` holder;
- same `Occupancy` post;
- five different `periodStart`/`periodEnd` pairs;
- assert five canonical IDs remain when run against clean resolver state.

That regression should pass with the current schema. Its purpose is to prevent the new broader temporal logic from recreating the historical bad merge.

## Repair

Issue #4134 also needs a resolver repair step. That is separate from the dedupe redesign:

1. Query canonical `Occupancy` edges with multiple distinct `periodStart` or `periodEnd` values.
2. Filter to cases where underlying source fragments have the same holder and post but different period ranges.
3. Generate a resolver repair list that explodes the affected clusters.
4. Run the repair after the regression test confirms clean-state edge-dedupe will not recreate the bad merges.

## Open Questions

- Is the initial `PROTECTED_PROPS` list too broad for any schema?
- Should any schema use a schema-equivalence class instead of exact-schema buckets?
- Do any date values in edge data fall outside parseable partial ISO dates, and should those be ignored or treated as incompatibilities?
