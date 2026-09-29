# prompt_evals

Evaluate a crawler's LLM extraction prompt against fixtures of human-accepted
[data reviews](../../zavod/docs/data_reviews.md), using
[pydantic-evals](https://pydantic.dev/docs/ai/evals/evals/).

Reviewers accept an extraction either as the model produced it or after editing
it. Both are ground truth for what the prompt should have produced. This tool
turns those accepted reviews into a fixture set that lives next to the crawler,
runs the crawler's *current* prompt over the original source documents, and
scores the result against what reviewers accepted. That lets a prompt change be
checked for regressions before it reaches production reviewers.

## Layout

```
contrib/prompt_evals/               the tool (dataset-agnostic)
datasets/<path>/<crawler dir>/evals/
    cases.yml                      the fixtures: one pydantic-evals Case per review
    cases_schema.json               generated JSON schema, gives editor support in cases.yml
    sources/<case>.html             the source document each case was extracted from
```

A case holds:

- `inputs`: the source file, its label and URL
- `expected_output`: the accepted `extracted_data`, stored as plain JSON so
  fixtures outlive model changes. Fields with no value are omitted rather
  than written as `[]` or `null`
- `metadata`: review key, when it was accepted, whether it was edited, one
  line per reviewer correction (written at export time, e.g. `removed:
  Turkish Company`), and hand-written `rules` slugs. The model's original
  extraction is not kept: it is output under a prompt that no longer exists. The
  reviewer's identity is not kept: it may help decide which reviews to turn
  into fixtures, but does not belong in one.

The crawler must expose the prompt and the response model as module attributes
of its entry point. By default the tool reads `PROMPT` and `MAX_TOKENS`, and the
response model is named with `--response-type`.

## Prerequisites

Run the tool from the repository root as a module (`python -m contrib.prompt_evals`),
and its tests as `python -m pytest contrib/prompt_evals`.

- The zavod virtualenv, plus `pip install -r contrib/prompt_evals/requirements.txt`.
- `ZAVOD_DATABASE_URI` pointing at a database with the `review` table for
  exporting fixtures. Model responses are cached in the same database's cache
  table, so repeating a run with an unchanged prompt makes no API calls.
- `OPENAI_API_KEY` for evaluation runs.

## Scoring

The dataset-level `ItemListMatch` evaluator pairs extracted items with accepted
items by name. Names are normalised for pairing with parenthesised acronyms and
quoted nicknames removed, so a policy change on those shows up as a
`name_exact` miss rather than a missing plus a spurious entity. It reports:

| Metric | Meaning |
|---|---|
| `recall` | share of accepted entities the prompt found; the reason lists missing names |
| `precision` | share of extracted entities that were accepted; the reason lists spurious names |
| `name_exact` | share of paired entities whose name string matches exactly |
| one score per field | share of paired entities where the field equals the accepted value (a missing field, `null` and `[]` are equal, list order and case are ignored) |
| `all_match` | assertion, passes only when everything above is perfect |

Fields present in fixtures but no longer in the model (for instance a
deprecated field) are ignored, so old fixtures keep working after a field is
removed.

## Choosing which reviews become fixtures

Every fixture is human-reviewed and every inconsistency between fixtures
confuses the prompt work, so the set should be as small as still covers every
rule several times. The `coverage` command derives rule slugs for each case
from its expected output and source text (which schemata appear, which fields
are non-empty, whether the source has parenthetical acronyms, quoted
nicknames, a vessel identified as blocked property, a State Department
co-designation, an enforcement agency acting as a bystander, and how many
designees: 0, 1, 2 or a large batch). Hand-written slugs in `metadata.rules`
add rules that cannot be derived, usually the rationale for a reviewer's
correction such as `unnamed-entity-skipped`.

```bash
python -m contrib.prompt_evals export  <dataset.yml> --response-type Designees --before 2026-09-08
python -m contrib.prompt_evals coverage <dataset.yml> [--per-case]
python -m contrib.prompt_evals select   <dataset.yml> --target 5 --max-items 25 \
    --keep sm606 --keep jy2473 --write
```

`select` greedily picks the fewest cases such that every rule is covered by
`--target` cases (or all that exist), keeping one case per press release,
skipping cases with more than `--max-items` designees unless named in
`--keep`, and preferring corrected cases and shorter ones on ties. `--write`
replaces the fixtures file with the selection and deletes unreferenced source
files. Name the known failures in `--keep`: a case where a prompt change lost
an entity is worth more than any derived tag.

To review the fixtures themselves, render them as one page per dataset:

```bash
python -m contrib.prompt_evals render <dataset.yml> --ide-link 'vscode://file/{path}:{line}'
open data/prompt_evals/<dataset>_fixtures.html
```

Each case shows the source document beside the accepted extraction, the
reviewer's corrections when there were any, its rule tags, and the fixture
path with the line number of the case so it can be opened in an editor. The
page is only for looking at fixtures; edit them in `cases.yml`.

After selecting, bring expected outputs in line with current policy before
committing. Fixtures encode the review policy at the time of acceptance, and
accepted mistakes exist (a prison accepted as a Vessel, an enforcement agency
accepted as a designee).

## Workflow 1: evaluate the current prompt

```bash
python -m contrib.prompt_evals evaluate datasets/us/ofac/press_releases/us_ofac_press_releases.yml \
    --response-type Designees --save data/prompt_evals/press_releases_$(git rev-parse --short HEAD).json
```

Read the report bottom-up: the `Averages` row gives the headline, the per-case
rows carry a `Reason` line naming each missing entity or mismatched value.
Useful options:

- `--case sb0290 --case sm719` runs named cases only, for quick iteration
- `--edited-only` runs only cases the reviewer had to correct, which is where
  the prompt was known to be wrong
- `--limit 20` for a cheap smoke test
- `--show-output` prints the full model output per case
- `--model gpt-4.1` evaluates the same prompt on another model
- `--save path.json` stores a summary to compare later with `--baseline`
- `--fresh` ignores cached responses, to measure how much an unchanged prompt
  varies between runs
- `--crawler-file path/to/crawler.py` evaluates the prompt in that file instead
  of the checked-out crawler, e.g. `git show other-branch:datasets/.../crawler.py
  > /tmp/candidate.py` to compare branches without switching

Run the evaluation once on the prompt currently in production and save it. That
summary is the baseline every candidate prompt is compared against.

### Caching

Responses are cached by prompt, response schema and source text in the zavod
cache table, which is the same cache the crawler uses. Evaluating the prompt
that is currently in production is therefore mostly served from the cache the
production crawl populated (when the database is a copy of production), and
re-running any unchanged prompt costs nothing. Only a changed prompt makes API
calls, roughly two cents per press release on gpt-4o.

### Reading the numbers

Model output varies between runs of the same prompt. Measure the noise floor
before judging a change: run the current prompt with `--fresh` and compare it
against its own saved summary. As a reference point, re-running the
production prompt for us_ofac_press_releases on gpt-4o over its own accepted
output misses entities in 19 of 179 cases. A change is real when it moves several cases in the same
direction, or when the per-entity field averages move by more than the noise
run did.

Per-case `all_match` is the noisiest metric because one wrong value fails the
case. The per-field averages are computed over every paired entity, thousands
of them, and are much more stable. Prefer them for go/no-go decisions.

## How many cases are enough?

There is no fixed number. Think in terms of coverage of behaviours and of the
precision you need on the headline rate:

- **Every rule in the prompt should have cases that fail without it.** Each
  instruction (strip acronyms, skip unnamed entities, Vessel is not the
  owning company, drop OFAC recent-actions links) should be exercised by at
  least a handful of cases, ideally five or more, so a regression on that rule
  is not a single flip that could be noise. A cheap check is to delete the rule
  and see which cases fail; if none do, the rule has no fixture coverage.
- **Edited reviews are worth more than unedited ones.** A case the reviewer
  had to correct is a documented model error; an unedited case mostly proves
  the model can repeat itself. Grow the set from edits (workflow 2), and add
  unedited cases mainly to protect recall on the common shapes of document
  (single-target releases, long multi-entity Russia batches, vessel lists).
- **Headline rates need many cases.** Detecting a five-point change in a pass
  rate near 40% at 95% confidence needs roughly 370 cases; a ten-point change
  needs about 90. With 179 cases you can trust shifts of eight points or more
  in `all_match`, while the per-entity field scores are precise well below
  that because each case contributes many entities.
- **Stop adding when new cases stop teaching.** Add accepted reviews in
  batches of twenty or so. When a batch reveals no new failure mode under the
  current prompt, the set is large enough for now; resume when reviewers start
  editing again.

## Workflow 2: add fixtures from newly accepted reviews

Every review a reviewer still has to edit is a case the prompt gets wrong. Once
accepted, it becomes a fixture:

```bash
python -m contrib.prompt_evals export datasets/us/ofac/press_releases/us_ofac_press_releases.yml \
    --response-type Designees --since 2026-09-08
```

`export` appends accepted, non-deleted reviews to `cases.yml`, skipping any
review key already present, and writes their source documents to `sources/`.
`--since` and `--before` filter on the review's `modified_at`, which is when it
was accepted or last edited. Commit the new cases together with the prompt
change that motivated them.

Fixtures encode the review policy at the time they were accepted. When policy
changes, for example acronyms moving out of the name field, the affected
expected outputs in `cases.yml` must be edited by hand or the case removed, or
the old policy will be scored as correct forever. The `metadata.accepted_at`
and `edited` fields help find which cases predate a policy change.

## Workflow 3: change the prompt without regressing

1. Run and save a baseline for the current prompt (workflow 1).
2. Edit the prompt in the crawler. Field descriptions on the pydantic model
   double as prompt text and as reviewer guidance in the review UI, so prefer
   changing those.
3. Evaluate against the baseline:

   ```bash
   python -m contrib.prompt_evals evaluate datasets/us/ofac/press_releases/us_ofac_press_releases.yml \
       --response-type Designees --baseline data/prompt_evals/press_releases_<old sha>.json
   ```

   The comparison lists every case whose metric moved, marked `improved` or
   `REGRESSED`, with the reason. Look at every regression: a drop in `recall`
   means an entity that reviewers accepted is no longer extracted.
4. Iterate on the prompt with `--case` on the regressing cases until the
   comparison shows no regressions. Model output is non-deterministic, so a
   single case flipping between runs of the same prompt is noise; a metric that
   drops across several cases is a real change.
5. When the change deliberately alters what a correct extraction looks like,
   update the affected fixtures (workflow 2) in the same change, and say so in
   the commit message.
6. Save the new summary as the next baseline.

Every run on an unchanged prompt and source hits the response cache, so the
cost of iterating is only the cases whose prompt text changed.
