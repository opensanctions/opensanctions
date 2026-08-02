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
