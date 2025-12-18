# Name cleaning

One or more names are generally available for entities named/listed in our data sources. We often need to

- categorise them, e.g. as primary name, aliases, or previous/former names,
- clean superfluous text that is not part of the name.

Clean, correctly categorised names are important to maximise recall (finding all true matches) and maximise precision (avoiding false positives).

While we've done this using simple, explainable logic for the most part, this leaves some noise or incorrectly-categorised names for a number of sources.

## What's a dirty name?

The helper [zavod.helpers.is_name_irregular][] returns true if a name potentially needs cleaning.

A dataset can customise what should be considered "in need of cleaning" using options under the `names` key of the dataset metadata. Each field under `names` is a schema type, so that different rules can apply to different entities in the dataset.

e.g.

```yaml
names:
  Company:
    reject_chars: ","
    allow_chars: "/"
```

#### ::: zavod.meta.names.NamesSpec
    options:
      members: []
      show_bases: false

#### ::: zavod.meta.names.CleaningSpec
    options:
      show_if_no_docstring: true
      show_bases: false

## Using LLMs

LLMs can do a lot of the categorisation and cleaning for us. We pair this with [human reviews](../data_reviews.md) to make 100% sure the categorisation and cleaning was correct, and did not lose any important information.


## Name cleaning helper

The helper [zavod.helpers.review_names][] makes it easy to

1. prompt for proper name categorisation and cleaning
2. get it reviewed

Once a dataset is fully reviewed, you can replace `review_names()` with [zavod.helpers.apply_reviewed_names][] which will

1. Call `review_names()` to do the cleaning and ensure a review exists
3. apply each extracted name to the correct property of an entity if the review is accepted
3. fall back to applying the original string cleaning wasn't deemed necessary, or human review is pending.


### What's a clean name?

#### weakAlias

- For Persons
    - single token e.g. `Foopie` or `John` but not `John Smith`.
    - Watch out for Chinese, Korean etc which don't have spaces - use an LLM or online translation to check namishness
    - Watch out Indonesian names can be single tokens. If in doubt, make it an `alias`

- For organisations
    - acronyms of their name e.g. `JSC SMZ` for `JOINT STOCK COMPANY SEROV MECHANICAL PLANT`
    - really short short forms
    - names where a significant part is a really common term, e.g. `TRO ITALIA` or `VA HOTLINE`

#### previousName

- Anything explicitly a previous name e.g.
    - `formerly ...`
    - `f/k/a`

#### alias

- Anything explicitly an alias, e.g.
    - `a.k.a`
    - `also ...`
    - for Persons, when it's obviously a nickname, e.g. `American Joe Miedusiewski`
    - for Organisations, we might capture some really vague names as `alias`, especially when a more distinctive complete alternative name is known.


- When variants are given, it's nice to expand the variants as aliases and keep the "primary" form, e.g.
```
- strings: ["Aleksandr(Oleksandr) KALYUSSKY(KALIUSKY)"]
  entity_schema: Person
  full_name: [Aleksandr KALYUSSKY]
  alias:
    - Oleksandr KALYUSSKY
    - Aleksandr KALIUSKY
    - Oleksandr KALIUSKY
```

#### name

- Anything else that doesn't clearly need splitting or categorisation
- If multiple names are given but not indicated to be aliases, treat them all as `name`.


#### middleName, fatherName, motherName

Some sources provide a bunch of names, e.g. name1, name2, ..., name6. A pattern we've seen is that

- name1 and name6, for example, are often reliably firstName and lastName respectively
- the sequence of names together make up an accurate representation of a full name
- the names in between might be middle names and patronymics, but we can't reliably categorise them.

The approach we take is

- construct a full name from the sequence (`h.make_name`) and add to the `name` property.
- assign `firstName` and `lastName`
- drop the remaining values on the principle that matching on those is supported via the full name, and dropping the name parts is better than mis-categorising them.


#### Splitting

- Transliterations given equal prominence can all be considered the same prop (if they're all in full)
```
- strings: ["Александар Добриндт / Aleksandar Dobrindt"]
  entity_schema: LegalEntity
  full_name:
    - Александар Добриндт
    - Aleksandar Dobrindt
```
- Watch out for place names at the end - it might just denote a branch. e.g. these are different ways of saying `Al-Qaida in Iraq` so don't split the location at the end
    - `The Organization Base of Jihad/Mesopotamia`
    - `The Organization Base of Jihad/Country of the Two Rivers`
- Especially in Person names, see if it's a cultural thing that's maybe one person's full official name
    - e.g. `Amir S/O AHAMED` means Amir son of Ahmed and appears to be one valid full name in Singapore

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
