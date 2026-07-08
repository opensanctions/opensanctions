# Matcher training pairs — data semantics

This file travels with every generated dataset (it is copied next to
`pairs.jsonl` on each run). If you are building or changing anything that
consumes this data — a training pipeline, an evaluation, an audit — read this
first. The data has non-obvious identity and independence semantics, and past
work that ignored them produced a leaky train/test split.

## What a row is

One row per **human judgement** recorded in the OpenSanctions deduplication
resolver: a decider looked at two entities and recorded `positive` (same
real-world entity), `negative` (different), or `unsure`.

Rows are produced by **chronological replay**: judgements are re-applied in
`created_at` order, and each row's `left`/`right` are the partially-merged
clusters *as the resolver knew them at the moment of that judgement* — before
the judgement itself was applied. A cluster judged five times as it grew
appears as five rows with progressively richer properties. Each row is
therefore (approximately) the evidence the decider actually saw, which is what
makes the labels trustworthy.

**Point-in-time approximation:** cluster *membership* is historical, but
record *content* is read from the current store. Records corrected, enriched,
or deleted since the judgement will differ from what the decider literally
saw; deleted records drop out entirely (see `skipped_missing`). Reconstructing
historical content would require statement-level history and is deliberately
out of scope.

## Identity rules — read this twice

- **Entity IDs are replay-time cluster IDs.** They are not source-record
  identities and not stable across rows: the same growing cluster changes ID
  as it merges. Never use them to deduplicate rows and never use them as a
  train/test split key.
- **`group` is the only leakage-safe split unit.** It labels the connected
  component of the full judgement graph — positive, negative, unsure, and
  automation edges alike — because all pairs in a component share evidence.
  Rows from one `group` must never straddle a train/test boundary. Splitting
  by row, by entity ID, or by exact content hash all leak: the replay emits
  many near-identical, highly correlated rows per cluster.
- **Repeated identical content is replay multiplicity, not source frequency.**
  If two rows have byte-identical entities, that means the cluster was judged
  twice in the same state — it is not evidence that the pattern is common in
  source data. Frequency-weighting on it measures decider behaviour, not the
  world.

## What is excluded, and why

| exclusion | reason | `summary.json` counter |
|---|---|---|
| edges by users `zavod/xref`, `opensanctions/xref`, `edge-dedupe` | matcher-driven judgements; training a matcher on matcher output is a feedback loop. Their merges still shape replayed cluster states — they are replayed, just not emitted. Rule-based `zavod/logic` judgements are deliberately kept in. | `skipped.automation` |
| `IGNORE_DATASETS` (see `generate.py`) | poor data quality / name-only matching; excluded from the store scope so their content cannot leak into merged clusters. Their pairs surface as missing entities. | part of `skipped.missing` |
| Address entities | not meaningful matching targets; skipped during cluster merging | `skipped.address` (address-only sides) |
| unresolvable sides | entity absent from the current store (deleted, renamed, out of scope) | `skipped.missing` |
| merge failures | incompatible schemata within a replayed cluster | `skipped.merge_error` |

## Field reference (`pairs.jsonl`, format_version 1)

| field | semantics |
|---|---|
| `left`, `right` | entity dicts: `id` (replay-time cluster ID), `schema`, `properties` with **sorted** value lists |
| `judgement` | `positive` / `negative` / `unsure` — `unsure` is emitted as-is; mapping or dropping it is a consumer decision, and it moves the negative class |
| `group` | connected-component label (smallest identifier in the component); the split unit |
| `source_id`, `target_id` | the judged edge's endpoints. Orientation is the resolver's canonical identifier ordering (`left` = target, `right` = source), not the order the decider saw |
| `created_at` | judgement timestamp; may be null (old edges) |
| `score` | matcher score attached to the edge, usually null on judged edges |
| `user` | first 12 hex chars of SHA-256 of the decider identity — supports per-decider error analysis without exposing identities; null if unrecorded |
| `left_datasets`, `right_datasets` | sorted source datasets contributing to each merged side |

`summary.json` holds the scan counters referenced above plus the component
size distribution. **Check the component stats before trusting a split**: if
one component holds a large share of pairs (negative edges chain clusters
together), the split policy for it is an open decision — measure, then decide.

## Determinism

Given the same resolver content and store content, output is byte-identical:
edges replay in `(created_at, edge key)` order, cluster members merge in
sorted order, and property value lists are sorted. Any diff between two
generated datasets therefore reflects a real change in judgements or source
data. Comparisons across regenerations are still not apples-to-apples for
*evaluation* — new judgements change groups and therefore splits.

## Open consumer decisions

- `unsure` handling: drop, map to negative, or down-weight (historically it
  was silently mapped to negative — an explicit choice is required).
- Split policy for very large components (mega-component risk): deferred until
  measured on real data.

## format_version changelog

- **1** — initial format: chronological replay, group labels, hashed users,
  lossless `unsure`, sorted serialization.
