from click.testing import CliRunner

from zavod.cli import run
from zavod.tests.conftest import VALIDATION_YML


def test_run_dataset():
    runner = CliRunner()
    result = runner.invoke(run, ["/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(run, [VALIDATION_YML.as_posix()])
    assert result.exit_code == 0, result.output
