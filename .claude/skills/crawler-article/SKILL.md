---
name: crawler-article
description: Scaffold a new crawler for an article-based source — press releases, enforcement notices, debarment or disciplinary announcements, news items — where each item is a narrative document and the entities have to be read out of prose rather than table cells. Creates the dataset .yml plus a crawler that walks a listing page to each article and emits Article and Documentation entities via make_article/make_documentation. Use when asked to add, write or scaffold a crawler for press releases, enforcement actions, or a source whose items are articles.
argument-hint: "[target path | source URL | GitHub issue URL]"
allowed-tools: Read, Edit, Write, Glob, Grep, Bash, WebFetch, WebSearch, Agent
---

# New Article Crawler

Create a crawler for a source whose unit of publication is a document rather than a
record. The user will provide a target path, source data URL, and/or a GitHub issue
URL: $ARGUMENTS

If given a GitHub issue URL, fetch it first to extract the data source URL and any
context about the dataset.

**Read upfront**:

1. `.claude/docs/crawler-guide.md` — shared crawler patterns (YAML, fetching, entities, helpers, lookups)
2. `zavod/docs/extract/enforcements.md` — the whole page. It is the reference for steps
   3–6: the age limit, Article/Documentation, reading entities out of prose, and the
   crash/warn/lookup rules.
3. `zavod/docs/data_reviews.md` — "Implementation" and "Review keys". Most article
   sources need the review framework, and the review key is the one decision in it that
   is expensive to change later.

**Consult on demand** (open only when you actually need the section — don't pre-load):

- `.claude/skills/crawler-article/examples.md` — code for the steps below.
- `zavod/docs/extract/names.md` — the `apply_reviewed_*` name helpers.
- `zavod/docs/metadata.md` — full YAML field reference.
- `zavod/docs/programs.md` — if the notices are sanctions designations.

**Ground the crawler in the files listed above — they are the only source you need.** The
wider crawler codebase is large and old, and many crawlers have drifted from current
practice, which is why the docs, not the corpus, are authoritative here.

## Step 1: Confirm the shape, then map the source

**Is this actually an article source?** It is, if the entities you want are named in prose
inside a document, and one document can name zero, one or many of them. If the index table
already carries the names, it's an ordinary table crawler — use `/crawler-sanctions`.

Establish these before writing code:

1. **The index**: what is one row, and where is the date — a cell, or only on the article?
2. **The order of the index.** Read the sort column, or the dates in the fetched page.
   This decides Step 3 and nothing else tells you.
3. **Pagination**: a `next` link, or a `?page=N` parameter?
4. **The article**: which element is the body, the title, the date. Fetch a few from
   different years — templates change and old notices keep the old one.
5. **The variants**: notices that won't parse like the rest (PDF-only, stubs, redirects).
   Each is a Step 6 decision, and finding them now is cheaper than in `issues.log`.
6. **The scope**: does the source publish articles outside OpenSanctions' scope, and does
   it tag them? A tag is your relevance lookup (Step 5); no tag means a weaker crawler,
   which the user should hear about.

## Step 2: YAML metadata — article-specific parts

Use the template in the crawler guide, plus:

```yaml
dates:
  formats: ["%d %B %Y"]      # REQUIRED: within_max_age raises ValueError on an unparsed date
ci_test: false               # LLM extraction: no API key in CI
assertions:
  min:
    schema_entities:
      Person: 170
      Article: 100           # assert Article counts too — they catch a broken index loop
  max:
    schema_entities:
      Person: 400
      Article: 250
```

- **`dates.formats` is not optional.** `within_max_age` parses with
  `fallback_to_original=False`, so an undeclared format raises on the first row. Add a
  `dates.months` block for non-English or abbreviated month names.
- **Assert `Article` counts.** An index loop that stops on page one still emits plausible
  Person counts; the Article count is what moves.
- **`coverage.frequency`**: `zavod/docs/metadata.md` has house defaults for sanctions, PEP
  and bulk sources, but none for notice streams. Set it from how often the source
  publishes; most existing article crawlers are `weekly`.

## Step 3: The index loop and the age limit

Full rules and code: `zavod/docs/extract/enforcements.md` → "Limit crawls to the
enforcement age limit". The decision:

- **Newest-first index** → the first out-of-age item ends the crawl. Have the per-page
  function return `bool` and `break` the pagination loop on `False`.
- **Any other order** → skip the item and carry on. Never `break`.
- **No date on the index** → the age check moves into the per-article function, after the
  fetch, and pagination has nothing to stop on.

Comment which case you're in and why — `return False` and `continue` look identical
otherwise. Keep `MAX_ENFORCEMENT_DAYS` (five years) unless a property of the source
forces otherwise, and comment the override.

## Step 4: Article and Documentation

Full rules: `zavod/docs/extract/enforcements.md` → "Create Article and Documentation
entities". In short:

- One `h.make_article` per notice, built and emitted once, outside the loop over the
  entities it names.
- One `h.make_documentation` per emitted `Thing`, related companies and vessels included.
- `Sanction` and relationship edges (`UnknownLink`, `Ownership`) are not `Thing`s and get
  no Documentation. Put the notice URL on `Sanction:sourceUrl`.

The signatures are `make_article(context, url, key_extra=None, title=None,
published_at=None)` and `make_documentation(context, entity, article, key_extra=None,
date=None)`. Don't invent parameters.

## Step 5: Getting entities out of the prose

Full ladder: `zavod/docs/extract/enforcements.md` → "Reading entities out of prose".
Take the first rung that works: deterministic parsing → a lookup → the review framework.
Don't reach past a rung that would do.

For the review framework, which is the normal answer here:

- Define the pydantic model with a `Schema = Literal["Person", "Company", "LegalEntity"]`
  field and `Field(description=...)` on anything ambiguous — the descriptions reach both
  the prompt and the human reviewer.
- **Key the review on a stable notice ID** (a release number from the URL) where the
  source has one, not the URL: URLs change when a site is reorganised and every review is
  then re-requested.
- Close `crawl()` with `assert_all_accepted(context)`. Use `raise_on_unaccepted=False`
  only when partial data is publishable, with a comment saying why.
- **Never** use a regex to split a list of defendants out of a sentence. A heuristic that
  *detects* an irregular string and routes it to review is fine; one that silently
  resolves it is not.

Names get `h.apply_reviewed_name_string` rather than a hand-rolled split.

## Step 6: Crash, warn, or add a lookup

Full rules: `zavod/docs/extract/enforcements.md` → "When a notice doesn't fit the expected
shape". The test is how many notices the deviation affects:

| Situation | Response |
|---|---|
| A structural selector (body, title, release ID) matches the wrong number of elements | **Crash.** `h.xpath_element` / `expect_exactly=` / `assert` with a message. |
| One notice deviates while its siblings parse — PDF-only, stub, no date | **`context.log.warning` + `continue`.** Never `assert` on an individual item. |
| A categorical value is new or unknown — notice type, entity type, topic | **Lookup**, with the miss loud. Never a bare `else` or default schema. |
| A value is well-formed but wrong — a misspelled month, an invented country | **`type.*` lookup.** |
| One specific notice must be excluded | **Lookup with a comment**, not a URL test in the crawler. |

Two things that aren't errors: an article naming **no entities** (filter out-of-scope
categories on the source's own labels, and let an unknown label warn), and a **new notice
type** where the parse still holds.

## Step 7: Validate

**A crawler that has not completed a successful `zavod crawl` is not deliverable.** If the
source can't be fetched, stop and report the blocker with the evidence from Step 1.

```bash
zavod crawl <path>          # then read data/datasets/<dataset>/issues.log — it must be clean
zavod export <path>         # runs the dataset validators and assertions
contrib/lint_dataset.sh <path>   # ruff + mypy + pre-commit as CI runs them; never invoke them bare
```

An LLM-extraction crawler reports unaccepted reviews on its first run. That is expected,
not a failure: run the [Review UI](https://github.com/opensanctions/opensanctions/tree/main/ui#readme)
and check the extractions are the right shape before handing over.
