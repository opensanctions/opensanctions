# Prompt tuning

Offline tooling for writing, optimising, and evaluating the LLM prompts used by the ETL
pipeline. This is **not** part of normal crawler development — it's carried out by the
platform team from time to time as necessary improvements are identified.

It lives in `contrib/` (and not in the `zavod` package) on purpose: we don't want
[DSPy](https://dspy.ai/) as a production ETL dependency. It pulls in a lot of extra
packages, and something in it interacts badly with leveldb (see the warning below). The
production code only ever loads the *resulting* prompt string, which is why the optimised
program JSON is checked in inside the `zavod` package while the tooling that generates it
is not shipped.

DSPy and its pin (`litellm`) are declared in `zavod`'s `dev` optional-dependencies, so a
`pip install -e ".[dev]"` in `zavod/` gives you everything these tools need.

## Layout

One subdirectory per tuned prompt. Today there's only one:

- `names/` — the single-entity name cleaning/categorisation prompt used by
  `zavod.extract.names.clean`. The optimised program is loaded at runtime from
  `zavod/zavod/extract/names/single_entity_program.json`.

## Running

Run everything as a module from the repo root (`~/Development/os/opensanctions`), so that
`contrib.*` and `zavod.*` both resolve:

```
python -m contrib.prompt_tuning.names.tune --help
```

### Optimising the prompt

The [GEPA optimiser in DSPy](https://dspy.ai/tutorials/gepa_facilitysupportanalyzer/)
develops an optimal prompt based on the example data and our feedback function
`contrib.prompt_tuning.names.optimise.metric_with_feedback`.

```
python -m contrib.prompt_tuning.names.tune optimise
```

Add `--level light` to use a subset of the data to experiment a bit more cheaply and
quickly.

Be careful not to try to make the feedback function too fancy. Big improvements have been
made by just looking at the prompt, and identifying when the examples result in ambiguous
or incorrect instructions. First check that there aren't mistakes in the example data, and
that there are enough examples of a scenario such that the randomly selected train, test,
and validation sets each have examples of it, so GEPA can develop instructions for all
similar but different cases.

The optimised program (in DSPy speak) is saved to
`zavod/zavod/extract/names/single_entity_program.json`. This contains the prompt and some
metadata. The fields to extract are defined in
`contrib.prompt_tuning.names.clean.CleanNamesSignature`.

### Evaluating the prompt (`compare`)

```
python -m contrib.prompt_tuning.names.tune compare validation_results.json
```

`compare` runs the held-out test set two ways and reports how they do:

1. **DSPy** — the optimised prompt executed through the DSPy client (as it was during
   optimisation).
2. **Direct GPT** — the same prompt run through the production path
   (`zavod.extract.names.clean.clean_names`), i.e. parsing the prompt out of the program
   JSON and calling the model directly, *without* DSPy.

This tells us two things: how well the optimised prompt performs on unseen data, and — more
importantly — that it works just as well through the production path as it does via DSPy.
We use the prompt directly in production, so we need the two to agree. Per-example detail
is written to the JSON output path; overall statistics are printed:

```
...
DSPy score: 43.660000000000004 out of 47 (92.8936170212766%)
Direct GPT score: 39.284 out of 47 (83.58297872340425%)
Agreement: 35.0 out of 47 (74.46808510638297%)
```

We probably want to be careful not to let the Direct GPT score go below 80. The scores
aren't precisely a percentage: 0 is given if none of the names are correct, 1 if all are,
and partial correctness results in a score in between. "Agreement" is when the same example
results in precisely the same output via DSPy and directly. Ideally add these lines to your
commit message when you update the prompt.

### Example data

Example data lives in `contrib/prompt_tuning/names/single_entity_examples.yml` and takes
the form:

```yaml
- string: "Ch'oe Ch'o'l-min (a.k.a Choe Chol Min) (DPRK individual)"
  full_name: [Ch'oe Ch'o'l-min]
  alias: [Choe Chol Min]
- string: "Cho Yan Nathan; a.k.a Nathan Man Man"
  full_name: [Cho Yan Nathan]
  alias: [Nathan Man Man]
```

`dump-examples` converts a Data Reviews CSV dump into this YAML format:

```
python -m contrib.prompt_tuning.names.tune dump-examples reviews.csv single_entity_examples.yml
```

## Tests

```
python -m pytest contrib/prompt_tuning
```

Run from the repo root so that `contrib.*` imports resolve.

## Warning: leveldb

Don't import DSPy into production ETL code.

Something in DSPy interacts with leveldb in a way that crashes when the process exits
unless leveldb is imported before DSPy. It looks like this:

```
src/tcmalloc.cc:309] Attempt to free invalid pointer 0x600002f2ede0
```

It appears to be caused by https://github.com/google/leveldb/issues/634. The entry points
here (`tune.py`, the tests) import `plyvel` before `dspy` to work around it.
