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

None of these necessarily intersect with the resolver — the entity queries involved can
just be `entity_id IN (...)` lookups on source ids.
