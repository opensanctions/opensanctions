---
name: refactor-crawler
description: Rewrite messy or AI-generated crawler code into clean, production-ready style that follows the zavod best practices. Use when the user asks to clean up, refactor, tidy, or "make production-ready" a crawler, or to bring code in line with best practices.
argument-hint: "[crawler.py path]"
allowed-tools: Read, Edit, Write, Glob, Grep, Bash
---

# Refactor crawler

Bring an existing crawler in line with current best practices. Typical input is a
first-draft or AI-generated crawler that works but is verbose, brittle, or non-idiomatic.
Target path: $ARGUMENTS

The rules live in `zavod/docs/best_practices/` — this skill is only the procedure for
applying them. When a rule is unclear, read the doc; do not invent a convention.

## Golden rule: every output change must be deliberate

Output may change when the refactor brings a methodological gain — e.g. moving to
`h.apply_date` (warnings + `original_value`), `h.make_address` composition, a
`type.address` lookup dropping placeholders, a tightened selector excluding junk. Name
each such change in your summary and tie it to the method that caused it. Never silently
alter emitted values. Leave name cleaning as it is: changing it means the sequenced,
multi-PR migration to the review system in `zavod/docs/extract/names.md`, out of scope
for a refactor pass.

## Prime directive: strict interpretation

`zavod/docs/best_practices/strict_interpretation.md` — every source value within the
crawler's scope is handled, explicitly ignored, or raises a signal. A refactored crawler
must be **more** likely to fail loudly than the draft it replaces, never less: when you
remove a branch, replace it with a guard, not with silence.

## The docs

- `best_practices/strict_interpretation.md` — the prime directive: destructive parsing, `audit_data`, assertions, categorical coverage
- `best_practices/patterns.md` — structure, naming, helpers, constants, logging, pagination, text hygiene
- `best_practices/xpath_and_html.md` — typed HTML helpers, selector quality
- `best_practices/entity_id.md` — `make_id` vs `make_slug`, which fields, `key=`
- `best_practices/datapatch_lookups.md` — replacing inline conditionals with YAML lookups
- `best_practices/dates_meta.md` — `apply_date` and dataset-level date formats
- `best_practices/addresses.md` — `make_address` + `copy_address`
- `best_practices/http_operations.md` — `context.fetch_*`, headers, Zyte
- `best_practices/caching.md` — what not to cache
- `best_practices/priorities.md` — which properties are worth the effort
- `best_practices/merge_checklist.md` — the pre-merge review checklist

For pure type-annotation errors, defer to the `/typechecker-fixes` skill.

## Workflow

1. **Read the crawler and its `.yml`.** Note the data source shape (HTML/CSV/XLSX/JSON/API)
   and the existing `lookups:`, `dates:`, and `http:` sections.
2. **Capture a baseline**: run `zavod crawl` on the unmodified crawler and keep the
   statement counts and a few sample entities to diff against later.
3. **Read the docs for the areas the crawler touches** and apply them, most impactful
   first: structure → selectors/parsing → IDs → dates/addresses → lookups → HTTP/caching →
   logging/assertions → style nits.
4. **Verify** (below).
5. **Summarise**: which best-practice areas changed, every output change and its
   methodological justification, and anything deliberately left alone.

## Verification

From the repo root:

```bash
ruff check --fix --select I <path/to/crawler.py>   # import order
cd zavod && mypy --strict <path/to/crawler.py>     # enforced by the mypy-datasets hook
zavod crawl <path/to/dataset.yml>                  # from the repo root
```

Then confirm:

- New strict-interpretation guards are welcome — but when one fires on today's data, add
  the missing lookup mapping rather than handing the warning off. The facility for many
  warnings stays in the code; `issues.log` is clean (transient network errors excepted).
- Diff the output against the baseline run (statement counts, spot-check entities) and
  **account for every difference** — each must trace to a named methodological gain.
- The crawler still satisfies its `assertions:` in the `.yml`.
- Walk the `merge_checklist.md` items relevant to what you touched.
