# tune

DSPy prompt optimisation and evaluation for the zavod name-cleaning prompt.

This lives outside `zavod/` deliberately: dspy and its dependencies (litellm,
and their openai version constraints) must not constrain zavod's own dependency
resolution, and zavod production code never imports dspy. See
`zavod/docs/extract/names.md` for the workflow this tool supports.

## Setup

The tool has its own uv environment in `contrib/tune/.venv`. On Linux, `uv run`
creates it on first use. On a Mac, see below first (🙄).

If you have another virtualenv active (e.g. zavod's), uv warns that
`VIRTUAL_ENV` doesn't match the project environment and ignores it. That's
expected. Don't pass `--active`, or dspy and litellm get installed into the
active environment.

### On a Mac

As for zavod (see [Dependencies on macOS](https://zavod.opensanctions.org/install/#dependencies-on-macos)),
pyicu must be built from source against the system ICU library:

        brew install icu4c
        cd contrib/tune/
        PATH="$(brew --prefix icu4c)/bin:$PATH" \
        uv sync --no-binary-package pyicu

## Run

From the repository root:

    uv run --directory contrib/tune tune.py optimise
    uv run --directory contrib/tune tune.py compare validation_results.json
    uv run --directory contrib/tune pytest

Real runs need `$OPENAI_API_KEY`.

By default, `optimise` writes the optimised program to
`zavod/zavod/extract/names/single_entity_program.json`. That file is a committed artifact.

## Warning

Don't import dspy into production ETL code.
