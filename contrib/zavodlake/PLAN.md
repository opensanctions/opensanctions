---
description: Experiment agenda for the zavodlake parquet statement lake
date: 2026-08-02
tags: [zavodlake, parquet, duckdb, prototyping]
---

# zavodlake: parquet statement lake experiments

The `build` command in this package backfills `statements.pack` and metadata for every
dataset in a collection and converts each pack into a deduplicated, typed
`data/lake/<dataset>/statements.parquet`.

## Idea

The fundamental idea is that these parquet files might be able to shift burden away from
the export, xref, analysis and enrichment processes — which today each fetch and load the
full statement set — towards re-usable, queryable artifacts.

## Experiments

1. **Parquet-backed nomenklatura Store.** Can the lake drive a `Store` implementation that
   is slower but runs off re-usable artifacts rather than fetching all statements? This
   will likely not work in export (graph traversal requires a lot of queries), but might
   work in xref, which only reads individual entities in its second phase.
2. **Faster LevelDB store builds.** Can a LevelDB store be built more quickly by feeding
   it from a parquet query instead of the pack files?
3. **Stateless xref decision tool.** Can the lake provide a backend store for an
   interactive web-based xref decision tool running as a stateless Cloud Run job?
4. **Inverted-index parquet.** Can we generate a second set of parquet files containing a
   tokenized inverted index à la `nomenklatura.index`, usable to drive xref and enrichment
   straight from bucket artifacts?
5. **Version deltas as anti-joins.** With two versioned statement parquets of the same
   dataset, statement-level added/removed/changed is a sorted anti-join — one duckdb
   query. Could this replace the delta export subsystem (`entities.delta.json`)?

None of these necessarily intersect with the resolver — the entity queries involved can
just be `entity_id IN (...)` lookups on source ids.

## Long-term layout

- Productized, the parquet file lives in each dataset's versioned `artifacts/` folder.
  ETL processes often run 8+ hours and need cross-query consistency, so readers pin
  full versioned paths at run start rather than globbing. Dataset versions have no
  relation to each other, so latest-per-dataset at start is a valid pin.
- The files stay unresolved: a PEP crawler runs monthly, but xref on PEPs runs nightly —
  merges cannot be stuck in that interval.
- Every published `statements.pack` becomes a parquet; collection scoping is a reader
  concern, not a builder concern.
- Pre-productization, a mirror under `contrib/zlake/<dataset>/<version>/statements.parquet`
  in the public bucket copies the final layout without writing to the prod zone; the
  `contrib/` prefix marks it as not prod material. It uses live run versions, just
  created outside the crawler run process. All versions are kept for now (no GC).
- The latest parquet may trail the latest released dataset version, so each build
  finishes by writing `contrib/zlake/<dataset>/parquet-latest.json` (version, path,
  rows, schema, built_at). The single-object PUT is atomic and builds per dataset are
  serialized by its crawl schedule, so the pointer is the source of truth; a
  `manifest.json` aggregating all pointers is a derived cache, rebuildable from a
  LIST. Later this becomes `parquets.json` in the artifacts root.
- The same event logic can drive a second artifact, the inverted-index parquet for
  xref (`blocker.parquet`): its build is triggered by the same bucket event, with
  versions tracked via a parallel pointer file (`blocker-latest.json` alongside
  `parquet-latest.json`).
- `latest.json` carries a schema version (`"schema": 1`) so readers can fail loudly
  before downloading anything. Additive columns don't bump it; any other change bumps
  it and implies a full rebuild.
- Bootstrap: a `sync` method in zavodlake reads the metadata catalog, checks for a
  parquet at the latest version, and if missing downloads the pack, transforms and
  uploads it. Later this can become event-driven (build on pack publish), which could
  replace both the planned zavod-export integration and the current
  load-statements-to-the-database mechanism.
