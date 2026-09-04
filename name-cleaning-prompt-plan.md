# Plan: improve LLM name cleaning (DSPy prompt, metric, gold examples)

Branch: `name-cleaning-prompt` (off main at dc365e6db). Not committed; scratch notes for
pausing/restarting. Working outputs go under `data/prompt_eval/names/` (gitignored).

## Context

- Production: `zavod/zavod/extract/names/clean.py` sends the DSPy-optimised prompt from
  `dspy/single_entity_program.json` directly to `LLM_MODEL_VERSION = "gpt-5.4"`, response type
  `SimpleNames` (name / alias / weakAlias / previousName / abbreviation lists).
- Reviews: `h.apply_reviewed_names` / `apply_reviewed_name_string` with `llm_cleaning=True`
  store `source_value = {"entity_schema", "original": {prop: [strings]}}`, `source_label="names"`,
  `original_extraction` = LLM output, `origin` = model name. Reviewer edits land in `extracted_data`.
- Tuning: `zavod-tune optimise` (GEPA, reflection LM gpt-5, student = production model),
  `zavod-tune compare <out.json>` prints "DSPy score / Direct GPT score / Agreement" on the
  test split; those three lines go into commit messages (see 53b889a33, c642e7369).
- Examples: `dspy/single_entity_examples.yml`, 387 examples (LegalEntity 255, Person 91,
  Company 26, Organization 15). Split 33/33/34 by `random.Random(0).shuffle` over the whole
  list, so adding examples reshuffles which examples are in the test set.
- Metric: `dspy/optimise.py::metric_with_feedback_dict`.
- Review DB snapshot: 2026-09-03 10:26. LLM-origin name reviews at latest version:
  gb_fca_notices 728 (56 accepted), ru_cbr_banks 386, afdb_sanctions 320, us_finra_actions 301,
  ebrd_ineligible 123, pg_parliament 104, fj_parliament 53, jp_sangiin 50 (21 acc), my_parliament
  22, tw_legislature 6, us_ddtc_debarred 5; iso9362_bic 59 (gpt-4o). Raw-JSON "edited" rates
  look very high (pg/fj 100%) - must compare with `Names.__eq__` semantics before trusting.
  Analyst-origin name reviews (tw_shtc 5394, jp_mof 1648, eu_journal 1589, ...) are also
  gold cleaning data but come from sanctions crawlers that don't use the LLM.
- Models reachable with the key include gpt-5.5, gpt-5.5-pro, gpt-5.4(-pro), gpt-5.2-pro.

## Steps (one commit each, compare scores recorded in every commit message)

### 0. Baseline (no code change)  [RUNNING 2026-09-03 17:18, ~130 examples, sequential]
`zavod-tune compare data/prompt_eval/names/00_baseline.json` on the current program/examples/
metric. Record the three lines. This is the reference for everything below.

### 1. Better comparison function  -> commit "Improve name cleaning metric"  [CODE DONE, tests+mypy pass; waiting for baseline to rescore, then commit]
Problems with the current metric:
- Extra-name penalty is `score *= 0.8` applied to the running total mid-loop, so the size of the
  penalty depends on which field is processed first and how many gold names precede it.
- A name put in the wrong field counts as both a miss and an extra (double penalty) and the
  feedback never says "miscategorised as alias, should be name", which is the most useful
  signal for GEPA.
- Extras are only penalised if not present in gold ignoring case, but misses give 0.7 for a
  casing difference: asymmetric.
- Feedback repeats "You correctly extracted..." for every hit; long, low signal.
Proposal (keep it simple, per docs warning):
- For each gold name, best match over the prediction: exact in same field 1.0; same field but
  casing/punctuation difference (slugify-equal) 0.7; present in a different field 0.3 with
  feedback naming both fields; absent 0.0.
- Extras = predicted names not slug-matching any gold name in any field. Score =
  matched_credit / (n_gold + n_extra). Order-independent, bounded [0,1], symmetric.
- Duplicate string across predicted fields counts as an extra once.
- Feedback lists only errors, one short sentence each; a single "All names correct." otherwise.
- Unit tests in `zavod/zavod/tests/extract/test_names.py` (existing tests cover partial/perfect).
Then re-run compare on the UNCHANGED program: `01_metric.json`. Score change here is purely the
metric. Commit with both baseline and new lines.

### 2. Stable train/val/test split  -> commit "Assign name examples to splits by hash"
Replace the global shuffle with a per-example deterministic assignment (hash of strings +
schema, 33/33/34). Adding examples then no longer moves existing examples between splits, so
score deltas across the later commits are attributable. Re-run compare: `02_split.json`.
(Skip if the user prefers to keep the current split; the caveat then applies to step 3.)

### 3. Gold examples from accepted reviews  -> commit "Add name examples from reviewed data"
Add `zavod-tune review-examples` (reads the review table directly, unlike `dump-examples`
which wants a CSV):
- Select latest-version, accepted, non-deleted, `source_label='names'`, origin like `gpt-%`
  (option to include analyst origin).
- Build candidate examples `{strings, entity_schema, name, alias, weakAlias, previousName}`
  from `source_value.original` and `extracted_data` (via `Names` so single-value/list and
  LangText forms normalise).
- Mark `edited` using `Names.__eq__` between original_extraction and extracted_data.
- Dedupe against existing examples by (sorted strings, schema).
- Write a report: edit rate per dataset, edit categories (recategorised name<->alias<->weakAlias,
  split/merged, punctuation stripped, dropped, added), and the LLM-wrong examples grouped by
  category, so we can pick and hand-check a set.
Selection: all LLM-wrong examples after hand-checking against the docs in
`zavod/docs/extract/names.md` (reviewers are not always right; see ch_seco experience), plus a
balancing sample of LLM-right ones from datasets/scenarios under-represented in the current
file (e.g. parliaments with honorifics, bank names, FCA notices). Keep the file grouped by
scenario with comments like the existing one.
Re-run compare with the unchanged program: `03_examples.json` shows how the current prompt does
on the enlarged test set.

### 4. Re-optimise with a stronger reflection model  -> commit "Optimise name cleaning prompt with gpt-5.5 reflection"
- `optimise.py`: reflection_lm gpt-5 -> gpt-5.5 (most capable non-pro model available; pro
  variants are slow and priced for single queries, not thousands of reflection calls).
  Student LM stays the production model so the prompt is tuned for what runs in production.
- Run `--level light` first for a sanity check, then heavy. Commit program + scores
  `04_optimised.json`.

### 5. (Optional, separate) Production model change
If desired, `LLM_MODEL_VERSION` -> gpt-5.5, then compare (`05_model.json`) and commit. Also a
candidate: re-optimise with gpt-5.5 as student. Cost/latency of gpt-5.5 in the crawlers needs
checking before committing this one.

## Open questions for the user
- Include analyst-origin reviews as gold examples, or only LLM-origin?
- Step 2 (stable split): do it, or accept moving test sets?
- Budget: heavy GEPA over ~500 examples with gpt-5.4 student + gpt-5.5 reflection is likely
  tens of dollars and 1-2 hours.

## Progress log
- 2026-09-03 17:18 baseline compare started -> data/prompt_eval/names/00_baseline.{json,txt,err}
- 2026-09-03 17:40 step 1 implemented: new metric in dspy/optimise.py, `zavod-tune rescore`,
  tests in tests/extract/test_names.py + test_tune.py, docs in names.md. Not committed yet.
- Review analysis (data/prompt_eval/names/reviews.pkl): 1456 accepted LLM-origin name reviews.
  Systematic reviewer edits the prompt does not cover: honorifics/post-nominals stripped
  ("Hon. X, MP" -> "X"; pg/fj parliament, 157); short forms and surname-only nicknames are
  weakAlias not alias (afdb 87, finra 46); "n/k/a"/"now known as" -> new name is `name`, old is
  previousName (finra 23, LLM has it backwards); legal-form abbreviation variants -> `abbreviation`
  (ru_cbr 67, afdb 23) BUT the signature has no abbreviation output at all; domain names kept as
  alias/name (gb_fca 35); other-script name in parentheses split into a second `name` (afdb);
  "(joint-stock company)" parentheticals kept in the name (ru_cbr); Malaysian a/l, bt patronymic
  variants added as extra name (my_parliament 11, conflicts with docs' Singapore S/O guidance).

## Progress log (continued) - 2026-09-04, paused for reboot

REMINDER (user): the ORDER of values within a list in name cleaning results never matters.
Only which key a string lands in and the exact string values matter. Any comparison, metric,
dedupe or diff must treat lists as sets. (The new metric and `Names.__eq__` already do.)

State at pause:
- Step 1 COMMITTED: dc5227341 "Improve the name cleaning metric and add zavod-tune rescore".
  Baseline outputs: data/prompt_eval/names/00_baseline.json (old metric scores in 00_baseline.txt,
  new-metric rescore in 01_metric_rescore.txt).
- Step 2 UNCOMMITTED, in working tree (tests + mypy pass):
  - zavod/zavod/extract/names/dspy/example_data.py: hash-based split (`split_for`), 33/33/34 by
    sha1 of schema + sorted strings. Real examples land train 126 / val 129 / test 115.
  - zavod/zavod/extract/names/dspy/compare.py: `print_scores` raises on empty test split.
  - zavod/zavod/tests/test_tune.py: 30 distinct examples so every split is populated; compare
    assertion now checks "should not be in alias".
  - A `zavod-tune compare data/prompt_eval/names/02_split.json` run was in progress when we
    paused (background, killed by reboot). RE-RUN it after reboot, then commit step 2 with the
    scores (note in the message that this is a one-time reshuffle of the test set, so scores
    are not comparable to 00/01; from here on they are).
  - Still to do for step 2: a sentence in zavod/docs/extract/names.md about the stable split.
- Step 3 NOT STARTED in code. The exporter draft (zavod/zavod/extract/names/dspy/review_examples.py,
  a `zavod-tune review-examples OUTPUT.yml REPORT.md --origin-like 'gpt-%'` command, tests in
  zavod/zavod/tests/extract/test_review_examples.py) was rejected before being written; the
  design is: pure `review_to_candidate(...)` converting one review row to
  {strings, entity_schema, name/alias/weakAlias/previousName/abbreviation} with an `edited`
  flag via `Names.__eq__` and rough edit-kind labels; DB query for latest-version accepted
  `source_label='names'` reviews; YAML grouped by dataset with LLM output + edit kind in
  comments; dedupe against the existing examples file by (schema, sorted strings); Markdown
  report of edit kinds. Analysis scratch data: data/prompt_eval/names/reviews.pkl.
- Steps 4 (re-optimise, reflection LM gpt-5.5) and 5 (production model) not started.

Open decisions still with the user: include analyst-origin reviews as gold; whether to add an
`abbreviation` output to the DSPy signature (reviewers use it heavily, LLM can't produce it);
policy on Malaysian a/l / bt patronymic variants; budget for the heavy GEPA run.

CORRECTION after checking the tree: the step 3 exporter files WERE written despite the tool
rejection message: zavod/zavod/extract/names/dspy/review_examples.py, the `review-examples`
command in zavod/zavod/tune.py, and zavod/zavod/tests/extract/test_review_examples.py exist,
UNTESTED and UNFORMATTED (ruff format/check, pytest, mypy not yet run on them). Do not stage
them with step 2. The 02_split compare run DID finish before the reboot; scores are in
data/prompt_eval/names/02_split.txt:
  DSPy score: 94.49166666666666 out of 115 (82.16666666666666%)
  Direct GPT score: 95.075 out of 115 (82.67391304347827%)
  Agreement: 93.0 out of 115 (80.8695652173913%)

## Decisions (user, 2026-09-04)
- Add `abbreviation` as a DSPy output field (own commit; examples may carry abbreviation values).
- Strip ALL honorifics and post-nominals (Hon., Mr, Mrs, Dr, Sir, Dato, MP, CMG ...). The afdb
  "Mr ..." accepted-as-is reviews are treated as reviewer errors.
- Malaysian a/l, bin, binti patronymics: one name, no variant (docs stand); exclude those edits.
- LLM-origin reviews only as example source for now.
- Committed so far on branch: dc5227341 metric+rescore, 547dfac72 hash split, ca8a46c6b review-examples.

## Progress 2026-09-04 (continued)
- Step 3a (abbreviation output) implemented in working tree: signature field, FIELDS, program JSON
  (field + instructions edits), docs (#### abbreviation; weakAlias org bullets), 8 existing
  examples relabelled weakAlias->abbreviation (IRGC ASF, IP SIM, VKS, JSC NIIP, CASC, CERS, SSRC x2),
  test mock. Compare running -> data/prompt_eval/names/03a_abbreviation.{json,txt}. Commit next.
- Step 3b curated examples drafted: data/prompt_eval/names/new_examples.yml (126 examples, from
  select_examples.py + hand exclusions; review listing in new_examples_review.txt). Append to
  single_entity_examples.yml AFTER the 3a commit, run compare -> 03b, commit.
- Step 3a COMMITTED fa614353e (abbreviation output). Step 3b COMMITTED c0af08fae (126 examples).
- Step 4: user rule: only `--level heavy` GEPA counts for real results (light is not used).
  Heavy run started 2026-09-04 01:25 detached (nohup) with reflection gpt-5.5, student gpt-5.4;
  logs data/prompt_eval/names/04_optimise_heavy.{txt,err}; writes single_entity_program.json.
  optimise.py (REFLECTION_MODEL = "gpt-5.5") uncommitted; commit together with the program JSON
  and the compare scores (04_optimised.json). Base valset score before optimising: 62.7%.
- Step 4 heavy GEPA (gpt-5.5 reflection) finished 01:34 in ~9 min: val 62.7% -> 83.7%, but the
  test split got WORSE: DSPy 75.2% (was 76.1%), direct 72.6% (was 76.9%). New-scenario examples
  improved (direct 55% -> 62%, DSPy 54% -> 76%) but existing ones dropped (83% -> 76%).
  Regressions (direct): 10 sole-input short all-caps org names now `name` instead of weakAlias;
  3 comma splits of single designations ("DEPUTY DIRECTOR, WOMEN'S BUREAU, DEPARTMENT OF LABOR");
  3 single-token persons to name; new prompt keeps "(CERS)" inside the name while also emitting
  the abbreviation. ROOT CAUSE: the examples file is itself split on sole-input short all-caps org
  names: 78 weakAlias vs 59 name (e.g. ZYFRA/TRUSTINFO weak, TRIBIT/YMV KREYN name), and the new
  gb_fca examples (ICMARKET, FALCONVEST -> name) tipped GEPA into "uppercase is never weak".
  NOT COMMITTED: single_entity_program.json (optimised), optimise.py (gpt-5.5). Results in
  data/prompt_eval/names/04_optimised.{json,txt}. Waiting on user decision on the labelling rule.

## Paused 2026-09-04 ~01:45 - how to resume
1. Decide the labelling rule for sole-input short all-caps org names (see root cause above):
   (a) generic/acronym-like -> weakAlias, distinctive brand -> name (old prompt's nuance; ~137
   examples to hand-check), or (b) all -> weakAlias (matches docs + 78 majority; relabel 59).
2. Relabel in single_entity_examples.yml, update the weakAlias/abbreviation docs to match,
   `zavod-tune compare` (unchanged program) and commit as its own step.
3. Re-run `zavod-tune optimise` (heavy only) -> compare -> commit if test improves over 03b
   (DSPy 76.1% / direct 76.9% on 150). Also check the CERS/SSRC regression (parenthetical acronym
   kept inside the name) and comma-splitting of single designations.
4. Optional step 5: production model gpt-5.5 (LLM_MODEL_VERSION), compare, commit separately.
The WIP commit below carries the gpt-5.5 reflection setting and the optimised-but-worse program
so it can be inspected; revert the program JSON to c0af08fae's version if starting over.

## Proposal: daily inconsistency report for reviewed extractions (2026-09-04, agreed in principle)

Loop we want: (1) find patterns where accepted reviews disagree -> (2) decide policy, fix the
minority rows in the review UI (changes published data) -> (3) re-export fixtures -> (4) optimise,
compare -> (5) deploy prompt, wait for more reviews -> back to (1).

### Slice 1: command + table (do first)
- `zavod review-patterns <task>` (or under zavod-tune), run daily by cron in the ETL cluster.
  Reads accepted reviews at each dataset's latest version for the task's datasets
  (names: source_label='names'; ch_seco: dataset ch_seco_sanctions) via ZAVOD_DATABASE_URI.
- Fingerprint = sha1 over sorted (dataset, key, extracted_data) of those reviews. Same
  fingerprint as the last stored report -> skip. (Edits and new accepts both change it.)
- Predicate registry, one module per task next to the extraction code, each entry:
  id, source regex / predicate over the source value, outcome function over extracted_data,
  policy text (may be empty = undecided), docs link. Seed = the ten sections in
  data/prompt_eval/review_worklist.md (generator: data/prompt_eval/make_worklist.py).
- Discovered candidates section: over EDITED reviews only, group edit kinds (recategorised A->B,
  split/trim, stripped prefix, added, dropped) with counts and sample rows. Humans promote these
  into curated predicates via PR. Keep tiers separate in the report.
- Every pattern reports BOTH sides: rows matching the predicate among unedited AND edited
  reviews, with the outcome distribution. (Lesson from this session: edited-only views turned a
  46/29 coin flip into "LLM wrong 40 times".) Also report how many production values a fix
  would touch.
- Table `review_pattern_report` in zavod.stateful.model: id, task, pattern_id, generated_at,
  fingerprint, policy (text), status (open / policy decided / fixed / won't fix), distribution
  (json), rows (json: dataset, key, review_id, source snippet, outcome, reviewer). Keep history
  (one row per pattern per generation) so distributions can be watched to unanimity.
- Completion signal for a pattern = distribution unanimous on the next run, not a checkbox.

### Slice 2: review UI page
- List patterns per task: distribution, status, policy text (editable), links to rows.
- Soft links resolved at render time: same review_id current -> link; key exists with newer id
  -> "changed since this report", link; key gone -> "stale, source changed", show stored snippet.
  Note key semantics differ: names key = sorted original strings (source edit => new key);
  ch_seco key = entity ssid (source edit => same key, new unaccepted revision).
- "Find more like this": scoped regex search on the list page (Postgres ~*), see
  review-ui-search-plan.md; add regex option there.
- "Create issue" button prefilled with pattern, distribution, policy text. No automatic issues.

### Per-pattern PR checklist (put in zavod/docs when slice 1 lands)
1. Policy line in docs (names: zavod/docs/extract/names.md "What's a clean name?"; ch_seco:
   crawler.py header + prompt).
2. Predicate marked policy-backed in the registry.
3. Fix minority rows in the review UI; confirm distribution unanimous on next report.
4. Re-export fixtures (build_fixtures.py / zavod-tune review-examples), commit with compare
   lines on the unchanged prompt.
5. Optimise (heavy GEPA only), compare, commit with lines; deploy.
