---
description: Research notes on lakehouse best practices and parquet/duckdb performance
date: 2026-08-02
tags: [zavodlake, parquet, duckdb, performance]
---

# Performance notes

Notes from reading up on lakehouse architectures and parquet/duckdb performance
(2026-08), plus local observations about our lake.

## Do we need a table format (Iceberg/Delta/Hudi)?

- Table formats earn their complexity through ACID transactions, concurrent writers,
  schema evolution and time travel. A single-writer pipeline producing immutable,
  batch-replaced artifacts gets most of that from a versioned-path convention — which
  the OpenSanctions archive already has (`artifacts/{dataset}/{version}/`).
- Consensus: plain parquet is fine until multi-writer transactions or governance are
  needed.
- If the prototype ever needs snapshots / multi-file consistency: **DuckLake** keeps
  data as plain parquet and puts all metadata in a SQL database (DuckDB, SQLite or
  Postgres — we already run Postgres). Lowest-friction upgrade path from where we are.

## Parquet layout numbers (duckdb guidance)

- File size: ideal 100 MB – 10 GB per file; broader lakehouse guidance says
  128 MB – 1 GB, well-tuned Iceberg tables use 256–512 MB.
- Row groups: sweet spot 100K–1M rows. <5K rows per group is 5–10× slower; 5K–20K
  still 1.5–2.5× off. DuckDB parallelizes *only across row groups*, so total row
  groups across scanned files should be ≥ CPU threads.
- Compression: zstd/snappy/lz4 recommended, gzip discouraged. (We use zstd.)
- The "small files problem" is the most-cited structural failure in production lakes:
  file count, not data volume, degrades planning, LIST calls and per-file open cost.
  Our lake: 447 files, one 1.7 GB, long tail of sub-MB files. Harmless locally; over
  a bucket a whole-lake glob pays ~450 HTTP round trips (footer fetch per file)
  before reading any data. Standard remedy is (sort-based) compaction into fewer,
  larger files.

## Remote querying (bucket-driven experiments)

- Against GCS/S3, network round trips dominate. DuckDB prunes at two levels: file
  elimination (hive partitioning / filename filters), then row-group elimination
  (zone maps). Both must engage for remote querying to be viable.
- **Sorting is the poor man's index.** We sort by `entity_id`, so min/max zone maps
  skip every row group that can't contain a looked-up entity.
- **Parquet bloom filters**: since duckdb 1.2.0 they are written automatically for
  dictionary-encoded columns (VARCHAR included) and probed on equality/IN
  predicates — measured ~50× on point lookups of absent values, ~47 bytes per row
  group overhead. Caveats: equality only; only written where dictionary encoding
  kicked in (`DICTIONARY_SIZE_LIMIT`, default 10% of row group size — `entity_id` at
  ~13 statements/entity may sit near that boundary; check with `parquet_metadata()`).
- Tension to keep in mind: per-dataset files are natural partitions for
  dataset-scoped work, but an `entity_id IN (...)` lookup across the whole lake pays
  per-file overhead 447 times. A consolidated artifact sorted on the lookup key
  would let the entity-read path touch one file and one or two row groups.

## Local benchmark (2026-08-02)

Point lookups against the full default lake (447 files, 127M statements, 3.9 GB) on a
laptop with local NVMe, duckdb 1.5.4:

| query | time |
| --- | --- |
| `entity_id IN (4 ids)` over the whole-lake glob, cold | 0.112 s |
| same, warm | 0.086 s |
| single entity from one dataset file | 0.004 s |
| `entity_id IN (1000 ids)` over the whole-lake glob | 0.308 s (~0.3 ms/entity) |
| `GROUP BY schema` aggregate over the whole lake | 0.080 s |

Bloom filters are present on all 475 `entity_id` column chunks of the largest file
(`ext_ru_egrul`, 58M rows), confirmed via `parquet_metadata()`.

Takeaways: batched entity reads are competitive with a key-value store locally;
per-entity glob lookups (~90 ms) are not — batch or consolidate. All numbers hide
per-file network round trips, so remote (bucket) behaviour needs its own measurement.

## Experiment 2 result: LevelDB build from parquet (2026-08-02)

Built the same nomenklatura LevelDB store for the `sanctions` scope (4.35M statements,
91 datasets) three ways (`contrib/zavodlake/exp2_leveldb.py`, local NVMe, warm files):

| variant | write | compact | total |
| --- | --- | --- | --- |
| pack feed (Store.sync baseline) | 16.7 s | 0.2 s | 16.8 s |
| parquet feed → Statement objects | 21.6 s | 0.5 s | 22.0 s |
| parquet feed → raw sorted key/value bytes | 17.2 s | 0.5 s | 17.7 s |

**Negative result: the build was never feed-bound.** CSV parsing is not the bottleneck;
per-row Python work and LevelDB writes dominate and every variant pays them. The
duckdb→arrow→pylist conversion is *slower* than the csv module, and even skipping
Statement objects entirely with pre-sorted keys only reaches par. Extrapolated, a full
`default` build is ~8–10 min regardless of feed. The parquet feed's value here is
convenience (deduped, typed input), not speed.

Fidelity note: the baseline store has 266 more `s:` keys than the parquet-fed ones —
duplicate statement ids with differing schema/content survive as distinct keys under
the pack feed, while the lake collapsed them at conversion time.

## Experiment 1 precursor result: xref read replay (2026-08-02)

Replayed an xref phase-2 read workload — 50k sampled entity ids, full entity assembly
via the same `store.assemble()` in every variant — against the `sanctions` scope
(`contrib/zavodlake/exp1_replay.py`, local NVMe):

| backend | ms/entity |
| --- | --- |
| LevelDB store, single gets (status quo) | 0.04 |
| parquet glob, single-id queries | 10.91 |
| parquet glob, 1000-id batches | 0.12 |
| duckdb table (indexed), single-id | 0.41 |
| duckdb table (indexed), 1000-id batches | 0.06 |

Materializing + indexing the 4.35M-statement scope into a duckdb table took **1.0 s**
(the LevelDB build of the same data took 17 s). All backends returned identical entity
counts.

Conclusions: batched parquet reads are viable for xref (3× LevelDB, ~60 s per 500k
entity reads — noise next to scoring); single-id glob access is not (280×). The
materialize-on-init duckdb table is the sleeper: near-LevelDB batched reads, tolerable
single reads, ~1 s startup per few million statements, no persistent store to sync —
and the right shape for a stateless Cloud Run job (download → materialize → serve).
At full-default scale (127M rows) the table wants a file-backed duckdb, not memory.

## Experiment 1 result: nomenklatura DuckDBStore (2026-08-02)

Implemented as `nomenklatura.store.duckdb_.DuckDBStore` (branch `pudo/duckdb-store` in
nomenklatura): read-only store over a caller-provided duckdb relation, read-time
canonicalisation from the Linker (`iter_pairs()` → canonical mapping table + resolved
edge table at init), bulk `View.get_entities()`. Validated against the sanctions-scope
lake (`contrib/zavodlake/exp1b_store.py`):

- store init, incl. canonical + edge table build over 4.35M statements: **0.12 s**
- `get_entities` batch-1000: **0.12 ms/entity** (exactly the exp1 raw-harness number —
  the Store API adds no measurable overhead), 50k/50k found
- `get_entity` singles: 10.4 ms/entity — bulk access is the contract
- inverted lookup (edge-table probe) and synthetic same-schema merge verified on real
  data; merges become visible by constructing a fresh store, no `update()` rewrite

Not yet done: batched prefetch in the nomenklatura xref loop, zavod integration.

## Alternative: materialize into a local duckdb table

Another possible way to play some of the experiments: instead of querying the
parquet files in place, materialize the full statements table into a local duckdb
database table, and run the experiment against that.

## Sources

- https://duckdb.org/docs/lts/guides/performance/file_formats
- https://duckdb.org/docs/current/data/parquet/tips
- https://duckdb.org/2025/03/07/parquet-bloom-filters-in-duckdb
- https://motherduck.com/blog/open-lakehouse-stack-duckdb-table-formats/
- https://motherduck.com/learn/what-is-a-data-lakehouse/
- https://clickhouse.com/resources/engineering/data-lakehouse
- https://hudi.apache.org/blog/2026/07/24/open-table-format-vs-data-lakehouse/
- https://thedatatoolbox.substack.com/p/a-quick-overview-on-ducklake-yet
- https://luminousmen.com/post/compaction-in-lakehouse/
- https://lakeops.dev/blog/iceberg-small-files-guide
- https://ved-prakash-sde.medium.com/querying-partitioned-parquet-files-on-gcs-with-duckdb-a-production-grade-guide-ffa8396ddb82
- https://github.com/duckdb/duckdb/discussions/13255
- https://medium.com/@connect.hashblock/duckdb-indexing-tricks-that-cut-my-latency-50d1bdb4efc3
- https://www.datobra.com/when-small-parquet-files-become-a-big-problem-and-how-i-ended-up-writing-a-compactor-in-pyarrow/
