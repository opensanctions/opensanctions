---
name: review-pep-dataset
description: Review a new or changed PEP dataset (a members-of-parliament, cabinet or judiciary crawler and its .yml) against the zavod docs, best practices and the /crawler-pep skill, before the manual final checks. Works as a self-review of a local dataset before opening a PR, or as a review of someone's PR, and drafts inline PR comments on request. Use when asked to review a PEP dataset, PEP crawler or PEP crawler PR.
argument-hint: "<PR URL or number | dataset .yml path>"
allowed-tools: Read, Glob, Grep, Bash, WebFetch, WebSearch, Agent
---

# Review a PEP dataset

Review the PEP dataset at: $ARGUMENTS

The goal is to settle everything a checklist can settle, so the human reviewer's final
check is short. This skill is only the procedure. **The rules live in `zavod/docs` and the
crawler skills; don't introduce new ones.** Every requested change must cite the doc it
applies, or a fact you verified in the source or the crawl output. Something that looks
wrong but no doc covers goes under "Questions for the reviewer", never as a change request.

## 1. Gather

**A PR** (URL or number):

```bash
gh pr view <n> --json title,author,body,headRefName,commits,files
gh pr diff <n>
git fetch origin <headRefName>
git show origin/<headRefName>:<path> | cat -n   # line numbers for inline comments
```

**A local dataset** (self-review): diff the dataset's directory, and any `meta/` files it
adds, against `origin/main`, including uncommitted changes.

Then:

- **Scope.** List every changed file. Point out files that don't belong to the dataset
  under review (another crawler, unrelated docs) and ask whether they belong in this PR.
- **Kind.** Check that the dataset is tagged `list.pep`. If it isn't a PEP dataset, stop
  and say this skill doesn't cover it.

## 2. Read the rules

- `.claude/skills/crawler-pep/SKILL.md`, with its `examples.md` ("Reference crawler")
  and `validation.md`
- `zavod/docs/peps.md`
- `zavod/docs/best_practices/merge_checklist.md`, `priorities.md`,
  `strict_interpretation.md` and `patterns.md`
- `zavod/docs/metadata.md` and `.claude/docs/crawler-guide.md`

Then read the docs for what the crawler touches: `entity_id.md`, `dates_meta.md`,
`addresses.md`, `datapatch_lookups.md`, `xpath_and_html.md`, `http_operations.md`,
`caching.md` and `name_titles.md` (all in `zavod/docs/best_practices/`), and
`zavod/docs/extract/names.md`. For a curated CSV in the dataset directory, also read
`best_practices/curated_datasets.md` and `best_practices/change_detection.md`.

### Use `/crawler-pep` as the checklist and the yardstick

The `/crawler-pep` skill is how this dataset should have been built, so review it the
same way:

- **Checklist.** Go through every bullet of crawler-pep Steps 1–3, and record for each
  one whether the dataset meets it, misses it, or it doesn't apply:
  - Step 1, source recon: endpoint, blocking, pagination, fields, terms, Wikidata,
    citizenship, a term-bounded signature, counts against reality
  - Step 2, YAML and crawler: tags, metadata skills, assertions, constants in the YAML,
    `topics=`, the order of person properties and emits, honorifics, name cleaning
  - Step 3, validation: crawl, issues, lint, status summary, integrity checks
- **Structure.** Compare the crawler side by side with the "Reference crawler" in
  crawler-pep's `examples.md`, and with the variant in that file that fits the source
  (mixed dataset, several positions, multi-term, subnational). Check the points under
  "What first drafts most often get wrong". Any helper, constant, guard or extra request
  that the reference doesn't have needs a reason a reviewer can see. If it has none,
  report it as machinery to remove or simplify.

## 3. Check the source

Don't take the crawler's comments on trust; check them against the source. Save
downloads in their own directory in the scratchpad and read them with `python -I`.

- Fetch `data.url` and every endpoint the crawler requests. Look for a structured
  endpoint behind the page (crawler-pep Step 1).
- List every field the source returns and how the crawler handles each one: emitted,
  explicitly ignored, or missing. A field that is silently dropped, or a lookup or
  constant that the code never uses, usually means data is missing from the output (for
  example, a role field that holds the presiding offices).
- Count the records against reality (seats, list size), by role and by term.
- Check every claim the crawler relies on: that a page lists all records, that a
  label is unique, that a date means what the comment says.
- Check Wikidata QIDs as crawler-pep Step 1 describes, and look for one where the
  crawler has none.
- For a curated dataset, compare the rows against the source documents (PDFs can be
  read with the Read tool).
- If a source can't be reached, record that and how you tried. Don't guess what it
  contains.

## 4. Run it

For a PR, copy the dataset directory into the scratchpad and crawl the copy, so the
user's checkout stays untouched. Locally, crawl in place.

```bash
zavod crawl <path/to/dataset.yml>      # then read issues.json in data/datasets/<name>/_artifacts/<version>/
zavod export <path/to/dataset.yml>     # validators and assertions
contrib/lint_dataset.sh <path/to/dataset.yml>
python .claude/skills/crawler-pep/scripts/pep_summary.py <dataset_name>
```

Then run the integrity checks in `.claude/skills/crawler-pep/validation.md`. Spot-check a
few entities against the source: they have names and the expected topics, and their
values match.

If the crawl can't run (a missing package, a blocked source), say so in the report.
Don't install packages.

## 5. Review in two passes

**Structural first**: anything that would change the crawler's shape or what it emits.
Missing entities or positions, the source choice, IDs, where dates come from, occupancy
status (crawler-pep Step 3), the curated file's conventions, change detection, and the
differences from the reference crawler found in step 2.

**Then the details**: walk `merge_checklist.md` line by line, then the metadata against
`metadata.md`, then the remaining docs from step 2. Also run the release checks from the
[crawler-planning README](https://github.com/opensanctions/crawler-planning#releasing-datasets)
that don't need a merge:

- the run has no errors and ideally no warnings
- the title, summary, description and publisher read sensibly, with valid Markdown and
  no spelling or grammar mistakes
- the entities have names and the expected topics

**Double-check each finding before reporting it.** Re-read the code path and test the
claim against the real data (for example, run the condition over every record). Drop
whatever you can't support. A wrong finding costs the author more than a missing one.

## 6. Report

In chat, briefly:

1. **Scope**: files that may not belong.
2. **crawler-pep checklist**: a compact table with one row per Step 1–3 bullet, saying
   met, missed or not applicable. Missed bullets also appear in the findings below.
3. **Structural**: numbered. Each item gives `file:line`, what is wrong, the evidence and
   the doc it applies.
4. **Smaller**: numbered, in the same form.
5. **Checked and fine**: one line each, so the reviewer knows what not to re-check.
6. **Couldn't verify**: what you couldn't check, and why.
7. **Questions for the reviewer**: things no doc covers.

Include the `pep_summary.py` table. For a self-review, stop here and
offer to fix the findings.

## 7. Draft inline comments (PRs only)

Do this when the user asks, and only for the points they pick. Write the review to
`<scratchpad>/review_<pr>.json`:

```json
{
  "event": "COMMENT",
  "body": "",
  "comments": [{"path": "datasets/xx/foo/crawler.py", "line": 42, "body": "..."}]
}
```

- `line` is a line number in the PR head's version of the file (step 1).
- Make one point per comment. Keep it short, polite, and phrased as a question or
  suggestion ("Could we …?"). Add the evidence when it isn't obvious (record names,
  counts).
- Leave the review body empty, or a single generic sentence, unless the user asks for
  more.
- Show the drafts in chat and **post only after the user explicitly approves**. Use
  `COMMENT` unless told otherwise. Never reply to existing comments.

```bash
SHA=$(gh pr view <n> --json headRefOid --jq .headRefOid)
jq --arg sha "$SHA" '. + {commit_id: $sha} | .comments |= map(. + {side: "RIGHT"})' \
  <scratchpad>/review_<pr>.json \
  | gh api -X POST repos/opensanctions/opensanctions/pulls/<n>/reviews --input - \
      --jq '{state, html_url}'
```
