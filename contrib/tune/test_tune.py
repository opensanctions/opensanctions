"""Very lightly check that the optimise/compare commands keep working."""

from pathlib import Path
from tempfile import NamedTemporaryFile, mkdtemp
from unittest.mock import MagicMock, patch
from copy import deepcopy
from traceback import format_exception

import pytest
import yaml
from click.testing import CliRunner, Result
from dspy import Prediction

from clean import CleanNamesSignature, ProductionFormatAdapter, load_optimised_module
from tune import cli

example = {
    "entity_schema": "Person",
    "strings": ["John Doe"],
    "name": ["John Doe"],
    "alias": [],
    "weakAlias": [],
    "previousName": [],
}
# Repeat 3 times because test/validation/train sets are each 1/3 of shuffled data
examples = [example, deepcopy(example), deepcopy(example)]


def assert_exit_status_zero(result: Result) -> None:
    if result.exit_code != 0:
        raise AssertionError(
            "CLI invocation exited non-zero.\n\n"
            f"Output: {result.output}\n"
            f"{''.join(format_exception(*result.exc_info))}"
        )


def test_production_format_adapter():
    """The adapter must mirror the exact wire format production uses:
    one user message, the prompt instructions and the input JSON as two
    text parts. The input framing lives in the tuned instructions."""
    messages = ProductionFormatAdapter().format(
        CleanNamesSignature,
        [],
        {"entity_schema": "Person", "strings": ["John (Johnny) Doe"]},
    )
    assert messages == [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": CleanNamesSignature.instructions},
                {
                    "type": "text",
                    "text": (
                        "{\n"
                        '  "entity_schema": "Person",\n'
                        '  "strings": [\n'
                        '    "John (Johnny) Doe"\n'
                        "  ]\n"
                        "}"
                    ),
                },
            ],
        }
    ]


def test_production_format_adapter_rejects_demos():
    """Demos cannot be rendered in the production format and must not pass
    silently: the shipped prompt must not depend on few-shot examples."""
    with pytest.raises(AssertionError, match="demos"):
        ProductionFormatAdapter().format(
            CleanNamesSignature,
            [{"strings": ["John Doe"]}],
            {"entity_schema": "Person", "strings": ["John Doe"]},
        )


@patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"})
@patch("optimise.dspy.GEPA")
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
    assert_exit_status_zero(result)

    with open(program_path) as f:
        program_data = f.read()
        assert "instructions" in program_data


@patch("compare.load_optimised_module")
def test_compare(mock_dspy_load: MagicMock):
    # Mock DSPy module prediction
    mock_optimised_module = MagicMock()
    mock_dspy_load.return_value = mock_optimised_module
    mock_optimised_module.return_value = Prediction(
        name=["John Doe"],
        alias=["Johnny"],
        weakAlias=[],
        previousName=[],
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
    assert_exit_status_zero(result)

    assert mock_optimised_module.called, mock_optimised_module.call_args_list

    with open(output_path) as f:
        program_data = f.read()
        assert "incorrectly" in program_data
