from datetime import timedelta

import pytest
from followthemoney.dataset import Version
from nomenklatura import duck
from rigour.time import datetime_iso

from zavod import settings
from zavod.archive import (
    ARTIFACTS,
    STATEMENTS_FILE,
    STATEMENTS_PARQUET,
    dataset_artifact_path,
    iter_statements_path,
)
from zavod.archive.backend import FileSystemObject
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.meta import Dataset
from zavod.publish import publish_dataset
from zavod.runtime.lake import build_statements_parquet, dump_statements_pack
from zavod.tests.util import make_context, run_dataset

SEEN_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _next_version(version: Version, monkeypatch: pytest.MonkeyPatch) -> Version:
    """Advance the process run version by a day, as a subsequent run would."""
    next_dt = version.dt + timedelta(days=1)
    next_version = Version.from_string(next_dt.strftime("%Y%m%d%H%M%S") + "-bbb")
    monkeypatch.setattr(settings, "RUN_VERSION", next_version)
    monkeypatch.setattr(settings, "RUN_TIME", next_version.dt)
    monkeypatch.setattr(settings, "RUN_TIME_ISO", datetime_iso(next_version.dt))
    monkeypatch.setattr(settings, "RUN_DATE", next_version.dt.date().isoformat())
    return next_version


def _seen_rows(dataset: Dataset, version: Version) -> list[tuple[str, bool, str, str]]:
    """(entity_id, external, first_seen, last_seen) for every parquet row."""
    path = dataset_artifact_path(dataset.name, version, STATEMENTS_PARQUET)
    with duck.connect() as conn:
        return conn.execute(
            f"""
            SELECT entity_id, external,
                strftime(first_seen, '{SEEN_FORMAT}'),
                strftime(last_seen, '{SEEN_FORMAT}')
            FROM read_parquet('{path.as_posix()}')
            """
        ).fetchall()


def test_build_and_dump_roundtrip(testdataset1: Dataset):
    version = settings.RUN_VERSION
    run_time = settings.RUN_TIME_ISO
    # The crawl builds the parquet and regenerates the pack from it.
    crawl_dataset(testdataset1, version)
    pack_path = dataset_artifact_path(testdataset1.name, version, STATEMENTS_FILE)
    original = {stmt.id: stmt for stmt in iter_statements_path(pack_path)}
    assert len(original) > 0

    # Duplicate a row: the pack writer only deduplicates within a batch, so
    # repeated ids can occur in real files and must collapse in the parquet.
    lines = pack_path.read_text().splitlines()
    with open(pack_path, "a") as fh:
        fh.write(lines[1] + "\n")

    build_statements_parquet(testdataset1, version)
    parquet_path = dataset_artifact_path(testdataset1.name, version, STATEMENTS_PARQUET)
    assert parquet_path.is_file()
    with duck.connect() as conn:
        conn.execute(
            f"CREATE VIEW lake AS SELECT * FROM read_parquet('{parquet_path}')"
        )
        duck.validate_statement_relation(conn, "lake")
        row = conn.execute("SELECT count(*), count(DISTINCT id) FROM lake").fetchone()
        assert row is not None
        assert row[0] == row[1] == len(original)
        entity_ids = [
            r[0] for r in conn.execute("SELECT entity_id FROM lake").fetchall()
        ]
        assert entity_ids == sorted(entity_ids)

    # No previous successful run: everything is first seen now.
    for _, _, first_seen, last_seen in _seen_rows(testdataset1, version):
        assert first_seen == run_time
        assert last_seen == run_time

    dump_statements_pack(testdataset1, version)
    regenerated = {stmt.id: stmt for stmt in iter_statements_path(pack_path)}
    assert set(regenerated.keys()) == set(original.keys())
    for stmt_id, stmt in regenerated.items():
        orig = original[stmt_id]
        assert stmt.entity_id == orig.entity_id
        assert stmt.schema == orig.schema
        assert stmt.prop == orig.prop
        assert stmt.value == orig.value
        assert stmt.dataset == orig.dataset
        assert stmt.lang == orig.lang
        assert stmt.original_value == orig.original_value
        assert stmt.origin == orig.origin
        assert stmt.external == orig.external
        assert stmt.first_seen == orig.first_seen
        assert stmt.last_seen == orig.last_seen


def test_first_seen_carried_over(
    testdataset1: Dataset, monkeypatch: pytest.MonkeyPatch
):
    first_time = settings.RUN_TIME_ISO
    # The crawl inside run_dataset builds the parquet, publish archives it.
    first_version = run_dataset(testdataset1)

    second_version = _next_version(first_version, monkeypatch)
    second_time = settings.RUN_TIME_ISO
    crawl_dataset(testdataset1, second_version)

    # The fixture data is unchanged, so every statement carries its stamp over.
    rows = _seen_rows(testdataset1, second_version)
    assert len(rows) > 0
    for _, _, first_seen, last_seen in rows:
        assert first_seen == first_time
        assert last_seen == second_time

    # Without a local copy, the previous parquet is read in place through the
    # archive object's URI, not backfilled:
    local_prev = dataset_artifact_path(
        testdataset1.name, first_version, STATEMENTS_PARQUET
    )
    local_prev.unlink()
    parquet_path = dataset_artifact_path(
        testdataset1.name, second_version, STATEMENTS_PARQUET
    )
    parquet_path.unlink()
    build_statements_parquet(testdataset1, second_version)
    assert sorted(rows) == sorted(_seen_rows(testdataset1, second_version))
    assert not local_prev.is_file()

    # An unreadable URI (e.g. access-restricted old objects) falls back to
    # backfilling the parquet:
    monkeypatch.setattr(
        FileSystemObject, "uri", lambda self: "file:///does/not/exist.parquet"
    )
    parquet_path.unlink()
    build_statements_parquet(testdataset1, second_version)
    assert sorted(rows) == sorted(_seen_rows(testdataset1, second_version))
    assert local_prev.is_file()

    # Drop the previous run's parquet locally and from the archive: the build
    # falls back to joining against its statements.pack.
    local_prev.unlink()
    archived = (
        settings.ARCHIVE_PATH
        / ARTIFACTS
        / testdataset1.name
        / first_version.id
        / STATEMENTS_PARQUET
    )
    archived.unlink()
    parquet_path.unlink()
    build_statements_parquet(testdataset1, second_version)
    assert sorted(rows) == sorted(_seen_rows(testdataset1, second_version))


def _emit_person(context: Context, name: str, external: bool) -> str:
    person = context.make("Person")
    person.id = context.make_slug(name)
    person.add("name", name)
    context.emit(person, external=external)
    assert person.id is not None
    return person.id


def test_external_first_seen_not_carried(
    testdataset1: Dataset, monkeypatch: pytest.MonkeyPatch
):
    first_version = settings.RUN_VERSION
    first_time = settings.RUN_TIME_ISO
    context = make_context(testdataset1, first_version)
    context.begin()
    external_id = _emit_person(context, "Jane Ext", external=True)
    internal_id = _emit_person(context, "John Int", external=False)
    context.finalize_statements()
    context.close()
    publish_dataset(testdataset1, first_version)

    second_version = _next_version(first_version, monkeypatch)
    second_time = settings.RUN_TIME_ISO
    context = make_context(testdataset1, second_version)
    context.begin()
    _emit_person(context, "Jane Ext", external=True)
    _emit_person(context, "John Int", external=False)
    context.finalize_statements()
    context.close()
    build_statements_parquet(testdataset1, second_version)

    rows = _seen_rows(testdataset1, second_version)
    assert len(rows) > 0
    for entity_id, external, first_seen, last_seen in rows:
        assert last_seen == second_time
        if entity_id == external_id:
            # External statements never donate their first_seen.
            assert external is True
            assert first_seen == second_time
        else:
            assert entity_id == internal_id
            assert external is False
            assert first_seen == first_time


def test_empty_run(testdataset1: Dataset):
    version = settings.RUN_VERSION
    context = make_context(testdataset1, version)
    context.begin()
    context.finalize_statements()
    context.close()

    build_statements_parquet(testdataset1, version)
    parquet_path = dataset_artifact_path(testdataset1.name, version, STATEMENTS_PARQUET)
    with duck.connect() as conn:
        conn.execute(
            f"CREATE VIEW lake AS SELECT * FROM read_parquet('{parquet_path}')"
        )
        duck.validate_statement_relation(conn, "lake")
        row = conn.execute("SELECT count(*) FROM lake").fetchone()
        assert row is not None and row[0] == 0

    dump_statements_pack(testdataset1, version)
    pack_path = dataset_artifact_path(testdataset1.name, version, STATEMENTS_FILE)
    assert list(iter_statements_path(pack_path)) == []


def test_missing_inputs(testdataset1: Dataset):
    version = settings.RUN_VERSION
    make_context(testdataset1, version)
    with pytest.raises(FileNotFoundError):
        build_statements_parquet(testdataset1, version)
    with pytest.raises(FileNotFoundError):
        dump_statements_pack(testdataset1, version)
