# tune

DSPy prompt optimisation and evaluation for the zavod name-cleaning prompt.

This lives outside `zavod/` deliberately: dspy and its dependencies (litellm,
and their openai version constraints) must not constrain zavod's own dependency
resolution, and zavod production code never imports dspy. The two sides share
exactly one file: the program artifact. See `zavod/docs/extract/names.md` for
the workflow this tool supports.

## Setup

The tool has its own uv environment in `contrib/tune/.venv`, with pure-Python
dependencies only. `uv run` creates it on first use on any platform.

If you have another virtualenv active (e.g. zavod's), uv warns that
`VIRTUAL_ENV` doesn't match the project environment and ignores it. That's
expected. Don't pass `--active`, or dspy and litellm get installed into the
active environment.

## The contract with zavod

`optimise` writes `zavod/zavod/extract/names/single_entity_program.json`.
Besides the serialised DSPy program, the artifact carries the contract that
zavod validates on load:

- `model`: the model the prompt was tuned for and is served with in production
- `input_fields` / `output_fields`: the fields the prompt works with

If the tuning tooling and zavod drift apart, zavod fails loudly on load
rather than mismapping fields.

## Running under production conditions

The prompt is optimised and evaluated through `ProductionFormatAdapter`
(`clean.py`), which formats LM calls exactly as production does: one user
message with the prompt instructions and the input JSON as two text parts,
and an OpenAI-style JSON-schema-constrained output (inherited from dspy's
`JSONAdapter`). This keeps optimisation honest: the prompt is not tuned inside
dspy's default chat scaffolding, which production never sends. The framing of
the JSON input is part of the tuned instructions in the artifact, so both
sides agree on it by construction; only the JSON serialization of the input
is written on both sides.

## Run

From the repository root:

    uv run --directory contrib/tune tune.py optimise
    uv run --directory contrib/tune tune.py compare validation_results.json
    uv run --directory contrib/tune pytest

Real runs need `$OPENAI_API_KEY`. LM responses are cached on disk (cache keys
include the prompt), so repeated compare runs don't re-bill.
