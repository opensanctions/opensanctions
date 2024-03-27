import shutil
from click.testing import CliRunner

from zavod import settings
from zavod.meta import Dataset
from zavod.dedupe import get_resolver
from zavod.cli import crawl, run, publish, export, clear, validate
from zavod.cli import load_db, dump_file, xref, xref_prune
from zavod.archive import dataset_state_path
from zavod.tests.conftest import DATASET_1_YML, DATASET_3_YML


def test_crawl_dataset():
    runner = CliRunner()
    result = runner.invoke(crawl, ["/dev/null"])
    path = settings.DATA_PATH / "datasets" / "testdataset1"
    assert result.exit_code != 0, result.output
    assert not path.exists()
    result = runner.invoke(crawl, [DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    assert path.exists()

    result = runner.invoke(clear, ["/dev/null"])
    assert path.exists()
    result = runner.invoke(clear, [DATASET_1_YML.as_posix()])
    assert not path.exists()


def test_export_dataset():
    runner = CliRunner()
    result = runner.invoke(export, ["/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(export, [DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    shutil.rmtree(settings.DATA_PATH)


def test_validate_dataset():
    runner = CliRunner()
    result = runner.invoke(validate, ["/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(validate, [DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    result = runner.invoke(validate, [DATASET_3_YML.as_posix()])
    assert result.exit_code != 0, result.output
    shutil.rmtree(settings.DATA_PATH)


def test_load_db():
    runner = CliRunner()
    db_path = dataset_state_path("x") / "dump.sqlite3"
    db_uri = "sqlite:///%s" % db_path.as_posix()
    result = runner.invoke(load_db, ["/dev/null", db_uri])
    assert result.exit_code != 0, result.output
    result = runner.invoke(load_db, [DATASET_1_YML.as_posix(), db_uri])
    assert result.exit_code == 0, result.output


def test_dump_file():
    runner = CliRunner()
    out_path = dataset_state_path("x") / "out.csv"
    result = runner.invoke(dump_file, ["/dev/null", out_path.as_posix()])
    assert result.exit_code != 0, result.output
    result = runner.invoke(dump_file, [DATASET_1_YML.as_posix(), out_path.as_posix()])
    assert result.exit_code == 0, result.output
    shutil.rmtree(settings.DATA_PATH)


def test_run_dataset(testdataset1: Dataset):
    latest_path = settings.ARCHIVE_PATH / "latest" / testdataset1.name
    assert not latest_path.exists()
    runner = CliRunner()
    result = runner.invoke(run, ["/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(run, ["--latest", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    assert latest_path.exists()
    assert latest_path.joinpath("index.json").exists()
    assert latest_path.joinpath("entities.ftm.json").exists()
    shutil.rmtree(latest_path)

    result = runner.invoke(publish, ["/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(publish, ["--latest", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    assert latest_path.exists()
    assert latest_path.joinpath("index.json").exists()
    assert latest_path.joinpath("entities.ftm.json").exists()
    shutil.rmtree(settings.DATA_PATH)


def test_xref_dataset(testdataset1: Dataset):
    runner = CliRunner()
    result = runner.invoke(crawl, [DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output

    resolver = get_resolver()
    assert len(resolver.edges) == 0

    result = runner.invoke(xref, ["--clear", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output

    get_resolver.cache_clear()
    resolver = get_resolver()
    assert len(resolver.edges) > 1

    result = runner.invoke(xref_prune, [])
    assert result.exit_code == 0, result.output

    get_resolver.cache_clear()
    resolver = get_resolver()
    assert len(resolver.edges) == 0

    shutil.rmtree(settings.DATA_PATH)
