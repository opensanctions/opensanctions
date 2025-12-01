"""Very lightly check that the optimise/compare commands keep working."""

# Load leveldb before importing dspy to prevent
# src/tcmalloc.cc:309] Attempt to free invalid pointer 0x600002f2ede0
# on exit. See: https://github.com/google/leveldb/issues/634
import plyvel  #  type: ignore  # isort:skip  # noqa: F401

from pathlib import Path
from tempfile import NamedTemporaryFile, mkdtemp
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner
from dspy import Prediction

from zavod.extract.names.clean import CleanNames
from zavod.extract.names.dspy.clean import load_optimised_module
from zavod.tune import cli

example = {
    "entity_schema": "Person",
    "strings": ["John Doe"],
    "full_name": ["John Doe"],
    "alias": [],
    "weak_alias": [],
    "previous_name": [],
}
# Repeat 3 times because test/validation/train sets are each 1/3 of shuffled data
examples = [example, example, example]


@patch("zavod.extract.names.dspy.optimise.dspy.GEPA")
def test_optimise(mock_gepa: MagicMock) -> None:
    """Very rough integration test of the optimise command."""

    mock_optimizer = MagicMock()
    # Actually return a previously-optimised module
    mock_optimizer.compile.return_value = load_optimised_module()
    mock_gepa.return_value = mock_optimizer

    # Create a temporary YAML file with a trivial example.
    # We're mocking the optimisation process, so the content doesn't really matter.
    # But if the file doesn't parse as yaml it should fail.
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


@patch("zavod.extract.names.dspy.compare.load_optimised_module")
@patch("zavod.extract.names.clean.run_typed_text_prompt")
def test_compare(run_typed_text_prompt: MagicMock, mock_dspy_load: MagicMock):
    # Mock DSPy module prediction
    mock_optimised_module = MagicMock()
    mock_dspy_load.return_value = mock_optimised_module
    mock_optimised_module.return_value = Prediction(
        full_name=["John Doe"],
        alias=[],
        weak_alias=[],
        previous_name=[],
    )

    # Mock direct OpenAI call
    run_typed_text_prompt.return_value = CleanNames(
        full_name=[],
        alias=["John Doe"],
        weak_alias=[],
        previous_name=[],
    )

    with NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(examples, f)
        examples_path = Path(f.name)
    output_path = Path(mkdtemp()) / "validation_results.json"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "compare",
            output_path.as_posix(),
            examples_path.as_posix(),
        ],
    )
    assert result.exit_code == 0, result.output

    assert mock_optimised_module.called, mock_optimised_module.call_args_list
    assert run_typed_text_prompt.called, run_typed_text_prompt.call_args_list
    assert result.exit_code == 0, result.output

    with open(output_path) as f:
        program_data = f.read()
        assert "incorrectly" in program_data
