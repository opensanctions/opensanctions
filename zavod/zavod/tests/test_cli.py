from click.testing import CliRunner

from zavod.cli import run, load_db
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
    result = runner.invoke(load_db, [db_uri, "/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(load_db, [db_uri, VALIDATION_YML.as_posix()])
    assert result.exit_code == 0, result.output
