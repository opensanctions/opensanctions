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
pyicu and plyvel must be built from source, otherwise importing plyvel fails with
`symbol not found in flat namespace '__ZTIN7leveldb10ComparatorE'`.

1. Ensure native libraries are installed (as usual for zavod):

        brew install icu4c leveldb

2. From the `contrib/tune/` directory, create the environment, building pyicu and plyvel from source:

        cd contrib/tune/
        PATH="$(brew --prefix icu4c)/bin:$PATH" \
        CPPFLAGS="-I$(brew --prefix leveldb)/include" \
        LDFLAGS="-L$(brew --prefix leveldb)/lib" \
        uv sync --no-binary-package pyicu --no-binary-package plyvel

3. Rebuild plyvel with `-fno-rtti`. `uv pip` targets an active `$VIRTUAL_ENV`
   rather than the project environment, so `--python .venv` is needed to
   install into the tune environment:

        CXXFLAGS="-fno-rtti" \
        CPPFLAGS="-I$(brew --prefix leveldb)/include" \
        LDFLAGS="-L$(brew --prefix leveldb)/lib" \
        uv pip install --python .venv --no-cache --no-binary plyvel --reinstall-package plyvel plyvel==1.5.1

4. Check it works:

        uv run python -c "import plyvel, icu, dspy; print('ok')"

Later `uv run` calls keep the source-built plyvel. If `uv.lock` changes the
plyvel version, `uv run` reinstalls it from a wheel; repeat step 3 with the
new version.

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

Something in DSPy interacts with leveldb in a way that crashes when the
process exits unless you load leveldb before importing dspy.

It looks like this:

```
src/tcmalloc.cc:309] Attempt to free invalid pointer 0x600002f2ede0
```

It appears to be caused by https://github.com/google/leveldb/issues/634
