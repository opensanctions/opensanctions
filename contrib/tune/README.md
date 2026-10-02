# tune

DSPy prompt optimisation and evaluation for the zavod name-cleaning prompt.

This lives outside `zavod/` deliberately: dspy and its dependencies (litellm,
and their openai version constraints) must not constrain zavod's own dependency
resolution, and zavod production code never imports dspy. See
`zavod/docs/extract/names.md` for the workflow this tool supports.

## Run

The tool has its own uv environment:

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
