"""Very lightly check that the optimise/compare commands keep working."""

from pathlib import Path
from tempfile import NamedTemporaryFile, mkdtemp
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner

from zavod.shed.names.dspy.split import load_optimised_module
from zavod.tune import cli


@patch("zavod.shed.names.dspy.optimise.dspy.GEPA")
def test_optimise(mock_gepa: MagicMock) -> None:
    """Very rough integration test of the optimise command."""

    mock_optimizer = MagicMock()
    # Actually return a previously-optimised module
    mock_optimizer.compile.return_value = load_optimised_module()
    mock_gepa.return_value = mock_optimizer

    # Create a temporary YAML file with a trivial example.
    # We're mocking the optimisation process, so the content doesn't really matter.
    # But if the file doesn't parse as yaml it should fail.
    examples = [
        {
            "string": "John Doe",
            "full_name": ["John Doe"],
            "alias": [],
            "weak_alias": [],
            "previous_name": [],
        }
    ]
    with NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(examples, f)
        examples_path = Path(f.name)
    program_path = Path(mkdtemp()) / "program.json"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "optimise",
            examples_path.as_posix(),
            program_path.as_posix(),
            "--level",
            "light",
        ],
    )
    assert result.exit_code == 0, result.output

    with open(program_path) as f:
        program_data = f.read()
        assert "instructions" in program_data
