# Name cleaning

One or more names are generally available for entities named/listed in our data sources. We often need to

- categorise them, e.g. as primary name, aliases, or previous/former names,
- clean superfluous text that is not part of the name.

Clean, correctly categorised names are important to maximise recall (finding all true matches) and maximise precision (avoiding false positives).

While we've done this using simple, explainable logic for the most part, this leaves some noise or incorrectly-categorised names for a number of sources.


## What's a dirty name?

The helper [zavod.helpers.name_needs_cleaning][] returns true if a name potentially needs cleaning.

A dataset can customise what should be considered "in need of cleaning" using options under the `names` key of the dataset metadata. Each field under `names` is a schema type, so that different rules can apply to different entities in the dataset.

e.g.

```yaml
names:
  Company:
    dirty_chars_extra: ","
```


::: zavod.meta.names.CleaningSpec

## Using LLMs

LLMs can do a lot of the categorisation and cleaning for us. We pair this with [human reviews](../data_reviews.md) to make 100% sure the categorisation and cleaning was correct, and did not lose any important information.


## Name cleaning helper

The helper [zavod.helpers.apply_reviewed_names][] makes it easy to

1. prompt for proper name categorisation and cleaning
2. get it reviewed
3. apply each extracted name to the correct property of an entity
3. fall back to applying the original string cleaning wasn't deemed necessary, or human review is pending.


## Prompt engineering

We use [DSPy](https://dspy.ai/) to write, optimise, and evaluate the prompt. The process is

1. Ensure we have good example data in `zavod/extract/names/dspy/single_entity_examples.yml`
2. Run `zavod-tune optimise` to find the ideal prompt for the data
3. Run `zavod-tune compare`
    - This shows us how well the prompt works on the validation set
    - It also shows us how well it works directly, compared with via the DSPy client.

We use the prompt directly, rather than via DSPy, to avoid introducing DSPy as a production ETL dependency with significant additional dependencies. There is also a bug in leveldb which interacts with something in DSPy, which is a bit scary to have to dance around in production code.


### Optimising the prompt

The [GEPA optimiser in DSPy](https://dspy.ai/tutorials/gepa_facilitysupportanalyzer/) is used to develop an optimal prompt based on the example data and our feedback function `zavod.extract.names.dspy.optimise.metric_with_feedback`

Run it using

```
zavod-tune optimise
```

add `--level light` to use a subset of the data to experiment a bit more cheaply and quickly.

Be careful not to try to make the feedback function too fancy.

Big improvements have been made by just looking at the prompt, and identifying when the examples result in ambiguous or incorrect instructions in the prompt. First check that there aren't mistakes in the example data, and that there are enough examples of a scenario such that the randomly selected train, test, and validation sets have examples of a scenario to let GEPA develop instructions for all similar but different cases.

Examples take the form

```yaml
- string: "Ch'oe Ch'o'l-min (a.k.a Choe Chol Min) (DPRK individual)"
  full_name: [Ch'oe Ch'o'l-min]
  alias: [Choe Chol Min]
- string: "Cho Yan Nathan; a.k.a Nathan Man Man"
  full_name: [Cho Yan Nathan]
  alias: [Nathan Man Man]
```

String represents the input string. The fields to extract are defined in `zavod.extract.names.dspy.clean.CleanNamesSignature`

The "optimised program" in DSPy speak is saved to `zavod/extract/names/dspy/single_entity_program.json`. This contains the prompt and some metadata.


#### Evaluate the prompt

Evaluate the optimised prompt by running

```
zavod-tune compare validation_results.json
```

Some progress information and overall statistics are printed, and details for each example are output to the provided JSON path.

```
...
DSPy score: 43.660000000000004 out of 47 (92.8936170212766%)
Direct GPT score: 39.284 out of 47 (83.58297872340425%)
Agreement: 35.0 out of 47 (74.46808510638297%)
```

We probably want to be careful not to let the Direct GPT score go below 80.

The scores aren't precisely a percentage, but 0 is given if none of the names are correct, 1 is given if all the names are correct, and partial correctness results in a score in between.

Agreement is when the same example results in precisely the same results via DSPy and directly.

Ideally add these lines to your commit message when you update the prompt.
