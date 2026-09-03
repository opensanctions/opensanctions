"""Prototyping workbench for a parquet-based statement lake.

Backfills dataset metadata and ``statements.pack`` files from the production
archive into the local data directory, then converts each pack file into a
typed, deduplicated ``data/lake/<dataset>/statements.parquet`` using duckdb.

Usage: ``python -m contrib.zavodlake build [DATASET]...``
"""
