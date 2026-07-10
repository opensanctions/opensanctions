# Name cleaning

One or more names are generally available for entities named/listed in our data sources. We often need to

- categorise them, e.g. as primary name, aliases, or previous/former names,
- split the string when multiple names are combined in one string, and
- clean superfluous text that is not part of the name.

Clean, correctly categorised names are important to maximise recall (finding all true matches) and maximise precision (avoiding false positives).

While we've done this using simple, explainable logic for the most part, this leaves some noise or incorrectly-categorised names for a number of sources.

In most cases, the end goal is to use the [zavod.helpers.apply_reviewed_name_string][] or [zavod.helpers.apply_reviewed_names][] to

- determine whether names need cleaning
- carry out some heuristic or llm-based cleaning
- create a [Data Review](../data_reviews.md)
- apply the cleaned or original names, depending on cleaning needs and review acceptance.

New crawlers can use the most appropriate of the `apply_reviewed_...` helpers straight away.

Existing crawlers which already do some splitting/cleaning can be [migrated to these helpers](#migrating-to-the-name-cleaning-helpers).

## Example usage

### Simple example, no existing cleaning

#### Before

```python
entity.add("name", row.pop("full_legal_name"), lang="rus")
```

Includes names like `THE NATIONAL BANK PLC (FORMERLY AL RAFAH MICROFINANCE BANK)` as a value of `name` property.

#### After

`crawler.py` replace `entity.add` with

```python
h.apply_reviewed_name_string(context, entity, string=legal_name, llm_cleaning=True, lang="rus")
```

`iso9362.yml` add

```yaml
names:
  schema_rules:
    LegalEntity:
      # Skip / because of 2913 A/S (company type) instances vs 1 C/O instance
      allow_chars: "/"
```

The LLM produced the following suggested extraction and this was proposed in a data review. Until it was accepted, the original value was applied to the entity. On the next crawl after the review was accepted, the names were applied to the correct properties as shown below.

```yaml
name: THE NATIONAL BANK PLC
alias: []
weakAlias: []
previousName: AL RAFAH MICROFINANCE BANK
abbreviation: []
```

### Multiple name fields in the source data

We add the strings to a `Names` instance and pass it to the name review system:

```python
# Names can be added to a names instance
original = h.Names(name=item["name"], previousName=item["former_name"])

# Multiple names can be added to a names instance
for alias in item["aliases"]:
    original.add("alias", alias["value"], lang=alias["language"])

# Then we can either just review the names
h.review_names(context, entity, original=original)

# Or we can review the names, applying the accepted cleaned/categorised versions if accepted,
# otherwise just applying the original strings in their original props
h.apply_reviewed_names(context, entity, original=original)
```

## Migrating to the name cleaning helpers

It can be nice to migrate existing crawlers which already do some cleaning themselves such that all the names cleaned through the helpers are fully reviewed when the switchover takes place. This is important because the original string(s) are applied as names when reviews are not accepted yet.

The goal of the migration is to remove all crawler-specific name cleaning and hand it off to the review system.

The approach in step 1 differs by dataset type:

- **Sanctions crawler**: pass the existing cleaned names as `suggested` with `default_accepted=True`, so the reviews are immediately accepted and output is unchanged while reviews accumulate.
- **Non-sanctions crawler**: add `llm_cleaning=True`, which creates LLM-cleaned reviews alongside the existing logic.

### Migration example

For a crawler that does some custom splitting:

```python
entity = context.make("LegalEntity")
names_string = row.pop("full_name")
entity.id = context.make_id(names_string, ...)

names = h.multi_split(names_string, ["a.k.a."])
entity.add("name", names[0])
entity.add("alias", names[1:])
```

#### Step 1

Introduce reviews alongside the existing logic without changing output.

**Sanctions crawler** — mirror existing cleaned names into `suggested` and auto-accept:

```python
entity = context.make("LegalEntity")
names_string = row.pop("full_name")
entity.id = context.make_id(names_string, ...)

original = h.Names(name=names_string)
suggested = h.Names()

names = h.multi_split(names_string, ["a.k.a."])
entity.add("name", names[0])
suggested.add("name", names[0])
entity.add("alias", names[1:])
for alias in names[1:]:
    suggested.add("alias", alias)

is_irregular, suggested = h.check_names_regularity(entity, suggested)
h.review_names(
    context,
    entity,
    original=original,
    suggested=suggested,
    is_irregular=is_irregular,
    default_accepted=True,
)
```

Before deploying the change, check that a sample of the created reviews look ok, and that the export doesn't have any changes to names.

You will need to deploy the step 3 change ASAP after running step 1 so that we don't default-accept new entities.

**Non-sanctions crawler** — add LLM cleaning:

```python
entity = context.make("LegalEntity")
names_string = row.pop("full_name")
entity.id = context.make_id(names_string, ...)

original = h.Names(name=names_string)

names = h.multi_split(names_string, ["a.k.a."])
entity.add("name", names[0])
entity.add("alias", names[1:])

h.review_names(context, entity, original=original, llm_cleaning=True)
```

Before deploying the change, check that a sample of the LLM-based extraction looks ok.

If the crawler defines its own list of alias-marker phrases (e.g. a `NAME_SPLITS` constant used to detect `"aka"`, `"d.b.a."`, `" or "` etc.), compare that list against `rigour.names.name_split_phrases_list()` and add any phrases not already covered as `reject_strings` in the dataset YAML under `names.schema_rules`. This ensures those patterns continue to flag irregularity once the old logic is removed in step 3.



#### Step 2

Once the crawler has run in production, complete the name reviews for this dataset.

#### Step 3

Remove all custom name cleaning and splitting logic and replace with a single `apply_reviewed_names` or `apply_reviewed_name_string` call.

**Sanctions crawler**:

```python
entity = context.make("LegalEntity")
names_string = row.pop("full_name")
entity.id = context.make_id(names_string, ...)

h.apply_reviewed_name_string(context, entity, string=names_string)
```

All crawlers calling `h.review_names` or `h.apply_reviewed_names` should end with a call to `assert_all_accepted(context, raise_on_unaccepted=False)` (imported from `zavod.stateful.review`). This logs a warning with the count of any name reviews that are still unaccepted for the current dataset and version, rather than raising, so the crawler completes and emits output while flagging that reviews remain outstanding.

After the deployment of step 3 has run, check the latest reviews and make sure new names were't auto-accepted between deploying step 1 and step 3.

**Non-sanctions crawler**:

```python
entity = context.make("LegalEntity")
names_string = row.pop("full_name")
entity.id = context.make_id(names_string, ...)

h.apply_reviewed_name_string(context, entity, string=names_string, llm_cleaning=True)
```


## What's a dirty name?

- `THE NATIONAL BANK PLC (FORMERLY AL RAFAH MICROFINANCE BANK)` (two names, one a `previousName`)
- `Aleksandr(Oleksandr) KALYUSSKY(KALIUSKY)` (a name and some alternative transliterations of the parts)
- `John Smith; Jonny Smith` (another form of multiple versions of a name in a single string)

The helper [zavod.helpers.is_name_irregular][] returns true if a name potentially needs cleaning. It can be used directly, but is also used by the other name cleaning helpers.

A dataset can customise what should be considered "in need of cleaning" using options
under the `names` key of the dataset metadata.

Schema-specific cleaning rules go under `schema_rules`, so that different rules can apply to different
entity types in the dataset.

`suggest_...` heuristics can be enabled to automatically suggest better categorisation for entity
types and name patterns. `h.review_names` and `h.apply_reviewed_...` include these heuristics.

e.g.

```yaml
names:
  schema_rules:
    Company:
      reject_chars: ","
      reject_strings: [" and ", " or ", " et "]
      allow_chars: "/"
  suggest_weak_alias_person_single_token: true
  suggest_abbreviation_uppercase_org_single_token_shorter_than: 8
  suggest_abbreviation_non_person_single_token_shorter_than: 5
```

#### ::: zavod.meta.names.NamesSpec
    options:
      show_bases: false

#### ::: zavod.meta.names.CleaningSpec
    options:
      show_if_no_docstring: true
      show_bases: false

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

## Using LLMs

LLMs can do a lot of the categorisation and cleaning for us. We pair this with [human reviews](../data_reviews.md) to make 100% sure the categorisation and cleaning was correct, and did not lose any important information.

!!! note

    We don't enable `llm_cleaning` for sanctions datasets. We prefer cleaning those manually and using deterministic heuristics.

## Prompt engineering

!!! note

    This is not part of normal crawler development. This is carried out by the platform team from time to time as necessary improvements are identified.

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
