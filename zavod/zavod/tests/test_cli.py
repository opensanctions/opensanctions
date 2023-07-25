from click.testing import CliRunner

from zavod.cli import run, load_db, dump_file
from zavod.archive import dataset_state_path
from zavod.tests.conftest import VALIDATION_YML


def test_run_dataset():
    runner = CliRunner()
    result = runner.invoke(run, ["/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(run, [VALIDATION_YML.as_posix()])
    assert result.exit_code == 0, result.output


def test_load_db():
    runner = CliRunner()
    db_path = dataset_state_path("x") / "dump.sqlite3"
    db_uri = "sqlite:///%s" % db_path.as_posix()
    result = runner.invoke(load_db, ["/dev/null", db_uri])
    assert result.exit_code != 0, result.output
    result = runner.invoke(load_db, [VALIDATION_YML.as_posix(), db_uri])
    assert result.exit_code == 0, result.output


def test_dump_file():
    runner = CliRunner()
    out_path = dataset_state_path("x") / "out.csv"
    result = runner.invoke(dump_file, ["/dev/null", out_path.as_posix()])
    assert result.exit_code != 0, result.output
    result = runner.invoke(dump_file, [VALIDATION_YML.as_posix(), out_path.as_posix()])
    assert result.exit_code == 0, result.output
