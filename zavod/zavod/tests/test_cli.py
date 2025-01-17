import shutil
from tempfile import NamedTemporaryFile

from click.testing import CliRunner
from nomenklatura import Resolver
from nomenklatura.db import get_engine, get_metadata
from sqlalchemy import MetaData, create_engine

from zavod import settings
from zavod.archive import dataset_state_path
from zavod.cli import cli
from zavod.integration import get_resolver
from zavod.meta import Dataset
from zavod.tests.conftest import DATASET_1_YML, DATASET_3_YML


def test_crawl_dataset():
    runner = CliRunner()
    result = runner.invoke(cli, ["crawl", "/dev/null"])
    path = settings.DATA_PATH / "datasets" / "testdataset1"
    assert result.exit_code != 0, result.output
    assert not path.exists()
    result = runner.invoke(cli, ["crawl", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    assert path.exists()

    result = runner.invoke(cli, ["clear", "/dev/null"])
    assert path.exists()
    result = runner.invoke(cli, ["clear", DATASET_1_YML.as_posix()])
    assert not path.exists()


def test_export_dataset():
    runner = CliRunner()
    result = runner.invoke(cli, ["export", "/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(cli, ["export", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    shutil.rmtree(settings.DATA_PATH)


def test_validate_dataset():
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(cli, ["validate", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    assert "No entities validated" in result.output, result.output
    shutil.rmtree(settings.DATA_PATH)


def test_load_db():
    runner = CliRunner()
    db_path = dataset_state_path("x") / "dump.sqlite3"
    db_uri = "sqlite:///%s" % db_path.as_posix()
    result = runner.invoke(cli, ["load-db", "/dev/null", db_uri])
    assert result.exit_code != 0, result.output
    result = runner.invoke(cli, ["load-db", DATASET_1_YML.as_posix(), db_uri])
    assert result.exit_code == 0, result.output


def test_dump_file():
    runner = CliRunner()
    out_path = dataset_state_path("x") / "out.csv"
    result = runner.invoke(cli, ["dump-file", "/dev/null", out_path.as_posix()])
    assert result.exit_code != 0, result.output
    result = runner.invoke(
        cli, ["dump-file", DATASET_1_YML.as_posix(), out_path.as_posix()]
    )
    assert result.exit_code == 0, result.output
    shutil.rmtree(settings.DATA_PATH)


def test_run_dataset(testdataset1: Dataset):
    latest_path = settings.ARCHIVE_PATH / "datasets" / "latest" / testdataset1.name
    artifacts_path = (
        settings.ARCHIVE_PATH
        / "artifacts"
        / testdataset1.name
        / settings.RUN_VERSION.id
    )
    assert not latest_path.exists()
    assert not artifacts_path.exists()
    runner = CliRunner()
    result = runner.invoke(cli, ["run", "/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(cli, ["run", "--latest", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    assert latest_path.exists()
    assert latest_path.joinpath("index.json").exists()
    assert latest_path.joinpath("entities.ftm.json").exists()
    # Validation issues in a published run are published
    with open(artifacts_path / "issues.json", "r") as f:
        assert "This is a test warning" in f.read()
    shutil.rmtree(latest_path)

    result = runner.invoke(cli, ["publish", "/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(cli, ["publish", "--latest", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    assert latest_path.exists()
    assert latest_path.joinpath("index.json").exists()
    assert latest_path.joinpath("entities.ftm.json").exists()
    # shutil.rmtree(settings.DATA_PATH)


def test_run_validation_failed(testdataset3: Dataset):
    artifacts_path = (
        settings.ARCHIVE_PATH
        / "artifacts"
        / testdataset3.name
        / settings.RUN_VERSION.id
    )
    assert not (artifacts_path / "issues.json").exists()
    runner = CliRunner()
    result = runner.invoke(cli, ["run", "--latest", DATASET_3_YML.as_posix()])
    assert result.exit_code != 0, result.output
    # Validation issues in an aborted run are published
    assert "Assertion failed for value" in result.output, result.output
    with open(artifacts_path / "issues.json", "r") as f:
        assert "Assertion failed for value" in f.read()
    shutil.rmtree(settings.DATA_PATH)


def test_xref_dataset(testdataset1: Dataset, disk_db_uri: str):
    runner = CliRunner()
    env = {"ZAVOD_DATABASE_URI": disk_db_uri}

    result = runner.invoke(cli, ["crawl", DATASET_1_YML.as_posix()], env=env)
    assert result.exit_code == 0, result.output

    resolver = get_resolver()
    resolver.begin()
    assert len(resolver.get_edges()) == 0
    resolver.rollback()

    result = runner.invoke(cli, ["xref", "--clear", DATASET_1_YML.as_posix()], env=env)
    assert result.exit_code == 0, result.output

    resolver = get_resolver()
    resolver.begin()
    assert len(resolver.get_edges()) > 1
    resolver.rollback()

    result = runner.invoke(cli, ["resolver-prune"], env=env)
    assert result.exit_code == 0, result.output

    resolver = get_resolver()
    resolver.begin()
    assert len(resolver.get_edges()) == 0
    resolver.rollback()
