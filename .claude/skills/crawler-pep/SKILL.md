---
name: crawler-pep
description: Scaffold a new PEP (Politically Exposed Persons) crawler — members of a parliament, legislature, senate, chamber of deputies, cabinet, judiciary, or an asset-declaration register — from a source URL or GitHub issue. Creates the dataset .yml plus a crawler emitting Person, Position and Occupancy entities via make_position/categorise/make_occupancy. Use when asked to add, write or scaffold a PEP or members-of-parliament crawler.
argument-hint: "[target path | source URL | GitHub issue URL]"
allowed-tools: Read, Edit, Write, Glob, Grep, Bash, WebFetch, WebSearch, Agent
---

# New PEP Crawler

Create a new PEP crawler. The user will provide a target path, source data URL,
and/or a GitHub issue URL: $ARGUMENTS

If given a GitHub issue URL, fetch it first to extract the data source URL and any
context about the dataset.

**Read upfront**:

1. `.claude/docs/crawler-guide.md` — shared crawler patterns (YAML, fetching, entities, helpers, lookups)
2. `zavod/docs/peps.md` — these three sections, which decide the shape of the crawler
   and are the ones most often missed. Read them as sections, not the whole file:
   `Grep` for the heading, then `Read` with `offset`/`limit`.
     - **"Properties to capture"** — the Must/Could/Won't ladder, and `citizenship`
       vs `country`.
     - **"Creating occupancies"** — `startDate`/`endDate` vs `periodStart`/`periodEnd`,
       when to let `make_occupancy` derive `status`, and `no_end_implies_current`.
     - **"Historical and multi-term sources"** — how far back to crawl, and the
       division of labour between `earliest_term_start` and `make_occupancy`.

**Consult on demand** (open only when you actually need the section — don't pre-load):

- `.claude/skills/crawler-pep/examples.md` — code examples (Patterns A/B/C, one label
  mapping to several held positions, multi-term sources, subnational variant,
  associates). Open when you're stuck on a pattern.
- `zavod/docs/peps.md` — the rest of it: position naming depth, `categorise()`, relatives.
- `zavod/docs/metadata.md` — full YAML field reference. Open if you're using a field not
  covered by the template in `crawler-guide.md`.
- `zavod/docs/best_practices/name_titles.md` — stripping honorifics (see Names below).
- `zavod/docs/extract/names.md` — open only if you're doing LLM-assisted or reviewed name cleaning.

**Ground the crawler in the files listed above — they are the only source you need.**
They are the curated, current best practice, and `examples.md` is the maintained version
of "show me a crawler like this one." The wider crawler codebase is large and old, so many
crawlers have drifted from current practice — which is exactly why the docs, not the
corpus, are authoritative here.

## Step 1: Understand the source

Do this before writing any code, and write the findings down as a short recon note —
you will need them for Step 2's field list and Step 3's pagination loop. Guessing at
this step is the most expensive mistake available: a wrong endpoint produces a crawler
that looks finished and is worthless.

**Reach the real data.**

- **Find the underlying JSON/XML endpoint before you parse any HTML.** Check the page
  source, the network calls, and the JS bundles. Official parliament sites very often
  have an open-data API behind the rendered page, and it carries more fields, more
  reliably, than the HTML does.
- **Prove the source is blocked before reaching for Zyte.** "Times out" and "returns
  nothing useful" are usually a missing header, not geo-blocking. In order: a browser
  `http.user_agent` in the YAML; a language cookie or `Accept-Language` (some sites
  serve their native language, and English names, party and region only with it); a
  format suffix (`.json`) or `Accept:` header. Only when all of those fail is it Zyte,
  which costs you `ci_test: false`.
- **Establish the pagination contract from the response**, not by guessing a `page=`
  parameter. Note the per-page cap and whether there is a `next` link to follow — an
  API that caps a page at 1000 while ignoring your `itens=5000` will silently truncate.

**Then map the content.**

- **Enumerate every field the source returns, and decide each one**: emit it, or name it
  in `audit_data(ignore=[…])`. Nested objects each get their own `audit_data` call.
  A field you never looked at is a field you will miss.
- **Enumerate the terms the source exposes, not just the current roster.** Is there a
  term switcher, an `ElectionId` parameter, a `/legislaturas` endpoint? See Step 3.
- Are start/end dates given per person, or only as term bounds?
- What are the position types (parliament, cabinet, judiciary, etc.)?
- Is there a Wikidata ID for the position(s)? (See `zavod/docs/peps.md`; skip QIDs for
  per-municipality / per-region positions.) Before using one, check on Wikidata that the
  item is `instance of (P31): position` and that its `applies to jurisdiction (P1001)`
  matches the country — a plausible label is not enough. The item's English label
  usually makes a good position name.
- **Term-bounded data?** Note any *structural* freshness signal (a new page URL, file
  name, or term id per term) so the crawler fails loudly when a new term lands.
  Record-count ranges are not a freshness signal — they belong in `assertions`.
- **Does the position legally require citizenship?** Don't assume from position type —
  national parliaments usually do (UK is an exception), but sub-national elected
  positions (mayors, councils) often don't. Spawn a subagent (`Agent` with
  `WebSearch`/`WebFetch`) to find the **legal document** (electoral law, constitution,
  official government guidance) that stipulates the citizenship requirement for this
  specific position. In a code comment next to the `person.add("citizenship", ...)`
  call — or, if citizenship is not required, next to the omission — include the URL to
  that legal document.

## Step 2: YAML metadata — PEP-specific parts

Full field reference: `zavod/docs/metadata.md`. PEP-specific additions:

```yaml
tags:
  - list.pep
```

- `coverage.frequency`: house default for PEP sources is `monthly` — see the frequency defaults in `zavod/docs/metadata.md`.
- **`assertions`: base the bands on what the crawl actually emitted, not on the
  chamber's seat count.** For the band widths themselves follow the rule of thumb in
  `zavod/docs/metadata.md`; the PEP-specific trap is the expected number it applies
  to. A crawler covering past terms holds several times the seat count and gains a
  cohort every election, so take the expected count from the run, not the
  constitution. Include `Position` counts when the crawler creates multiple position
  types.
- `ci_test: false` needs a comment saying why (Zyte, or a runtime over the 2-minute CI
  budget) — see the maintainer-notes convention in `zavod/docs/metadata.md`.
- Lookups rarely go past `type.*` for PEP crawlers. Non-English role labels are handled
  by `translate_name=True` in `make_position`; a `position` translation lookup is only
  worth it when the source has very few distinct labels. A non-`type.*` lookup needs a
  comment above it explaining what it matches and what the crawler does with the result.
- **Constants belong in the YAML, not the crawler.** A gender map, request headers, a
  user agent, date formats or column labels go into `lookups` / `http` / `dates` /
  `config` — use `/crawler-constants-to-yml` if you have already written one in code.
- For `title`, `description` and `coverage.frequency` on a legislature dataset, apply
  `/legislature-metadata`, and `/dataset-metadata` for the remaining fields, rather
  than inventing a house style here. Note that per-record field lists ("records each
  member with their name, party and date of birth") do not belong in `description`.

## Step 3: Write the crawler module

### Required imports

```python
from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
```

### Person properties

Capture properties by priority — don't chase every field. For people, capture when
available: name(s), date/place/country of birth, citizenship/nationality, and ID
numbers. Don't extract private addresses or phone numbers. Full PEP property ladder
(Must/Could/Won't): `zavod/docs/peps.md` → "Properties to capture".

On the **occupancy**, `constituency` and `politicalGroup` (the parliamentary faction,
distinct from party membership in `Person:political`) are valuable where the source
gives them.

### Names

Strip honorifics deterministically via dataset config, not in code — declare them in a
`names.prefixes_strip:` block in the YAML and call `h.strip_name_titles`, keeping the
raw form as `original_value` so nothing is lost:

```python
clean_name = h.strip_name_titles(context, raw_name)
if clean_name is None:
    return
person.add("name", clean_name, lang="eng", original_value=raw_name if clean_name != raw_name else None)
```

Full reference: `zavod/docs/best_practices/name_titles.md`. LLM-assisted
(`h.clean_names()`) and reviewed-name (`h.apply_reviewed_names()`) helpers are both
acceptable for PEP data — `zavod/docs/extract/names.md`. (Unlike sanctions, where LLM
cleaning is forbidden.)

### Position naming

Build position names with `h.make_position`. Rules:

- **Always pass `lang=`** (ISO 639-3, e.g. `lang="eng"`, `lang="fra"`) declaring the
  language the position name is in. If omitted, `make_position` falls back to the
  dataset's `data.lang` (`lang or context.lang`) — so an English name over a
  non-English source must set `lang="eng"` explicitly. Two cases:
    - **Crawler-supplied names** (the standard case — e.g. a parliament crawler where
      the name is always `Member of the ... Parliament`): write the name in English
      and pass `lang="eng"`. Use the standard English term for the role; keep
      native-language terminology only for proper nouns of specific institutions
      (e.g. `Landtag of Mecklenburg-Vorpommern`). Pass `lang="eng"` even when the
      dataset's `data.lang` is another language (e.g. a `data.lang: spa` source whose
      crawler emits `Member of the Congress of the Republic` still passes
      `lang="eng"`) — otherwise the English name is treated as being in the dataset
      language and, with `translate_name=True`, wrongly sent to the translator.
    - **Source-supplied names** (role labels read from the data): pass them through
      as-is with the source language as `lang=` and `translate_name=True` —
      `make_position` translates the name to English via LLM and keys the entity ID
      on the untranslated original, so the ID stays stable. Only when the source has
      very few distinct labels, a `position` YAML lookup translating them to English
      (then `lang="eng"`) is fine instead — see the subnational variant in
      `examples.md`.
- Include the role, the organisational body where relevant, and the geographic jurisdiction. For members of national parliaments, include `citizenship` (except UK Parliament).
- A national position's name must be recognizable as belonging to that country when read
  on its own — either a nationality adjective (`Member of the Swedish Riksdag`) or an
  of-phrase (`Member of the Senate of the Italian Republic`).
- **Pass `topics=`** for positions the crawler names itself (`["gov.national", "gov.legislative"]`,
  `["gov.state", ...]` for sub-national, `gov.executive`/`gov.judicial` by branch). Omit them
  for positions read out of the source data, where the review and classification system
  decides. Vocabulary:
  https://www.opensanctions.org/docs/pep/methodology/
- Avoid: legislative term, an elected official's constituency, or the country for sub-national representatives.
- `wikidata_id` becomes the position's entity ID, so never pass the same QID to multiple distinct positions — they'd collapse into one entity. Per-municipality/region positions usually omit `wikidata_id` (per-locality QIDs rarely exist on Wikidata) and rely on `subnational_area=...` to disambiguate; pass a QID only when each subnational position has its own unique Wikidata entry.

Depth on edge cases: `zavod/docs/peps.md` → "Selecting a position name".

### Position categorisation

`categorise()` is a stateful DB operation; `is_pep`/`topics` only matter on first
insertion — subsequent crawls return DB values (including UI edits).

`default_is_pep` defaults to `True`, so **`categorise(context, position)` is the
idiomatic call** — don't pass `default_is_pep=True`. Pass `default_is_pep=None` only for
a mixed dataset or per-locality positions where the UI decides PEP status.

When `categorisation.is_pep` is false, emit nothing for that position: `return` or
`continue` past it. Always — never `raise`. Gate on it even with the default
`default_is_pep=True`, since the position may have been un-flagged in the review UI.
Pass the returned `categorisation` to `make_occupancy()`.

### Historical terms

**Default to covering past terms whenever the source exposes them** — a national
legislator stays politically exposed for years after leaving office, and this is the
single most commonly missed requirement in PEP crawlers. Discover the terms in Step 1;
then, per `zavod/docs/peps.md` → "Historical and multi-term sources":

- Bound the crawl with `h.earliest_term_start(TOPICS)`, walking terms newest-first and
  stopping once one ended before the cutoff. Log a skipped term at `info`.
- Put whole-term bounds in `period_start`/`period_end` and per-person mandate dates in
  `start_date`/`end_date`. They can coexist.
- Derive `no_end_implies_current` **per occupancy** (e.g. `term.period_end is None`),
  not once for the dataset.
- Let `make_occupancy` derive `status` from the dates; override it only when the source
  states a mandate is current or ended but gives no date.

Worked example: `examples.md` → "Multi-term source".

### Critical rules

- Set ALL person props (birthDate, deathDate, etc.) BEFORE calling `make_occupancy()` — it reads them to determine PEP status.
- `make_occupancy()` returns `None` if the occupancy doesn't meet PEP criteria. Only emit persons with at least one valid occupancy.
- Emit the person AFTER `make_occupancy` — it mutates `person.topics`. Don't add `role.pep` yourself.
- For judicial crawlers, also `person.add("topics", "role.judge")`.
- **Term-bounded sources** (fixed mandates, per-term archives): fail in `crawl()` when the source's *structural* signature changes (new page URL, file name, term id). A continuously-updated roster (a parliament refilled by by-elections) is not term-bounded.
- **No hand-rolled total-count guard.** Don't close `crawl()` with `if not seen: raise ValueError("no members")` — the metadata `assertions` block owns totals, and a guard on the total needs editing every time the source grows. Guard individual selections instead (`h.xpath_element` already raises on zero matches). See `zavod/docs/best_practices/strict_interpretation.md`.

## Step 4: Validate

**A crawler that has not completed a successful `zavod crawl` is not deliverable.** If
the source cannot be fetched — geo-blocked, Zyte unavailable, anti-bot — stop and report
the blocker with the evidence from your Step 1 recon note. Do not ship a parser
validated against an archived copy, and do not report the task complete. A real run is
what catches the wrong-endpoint mistake that no amount of code review will.

```bash
zavod crawl <path>          # then read data/datasets/<dataset>/issues.log — it must be clean
zavod validate <path>
contrib/lint_dataset.sh <path>   # ruff + mypy + pre-commit exactly as CI runs them
```

Never invoke bare `ruff` or `mypy` instead of the script — without the repo config they
report findings CI does not enforce, and miss the per-dataset exclusions it applies.

Then read your own diff against these, the defects that most often survive to review:

- Every source field is either mapped or named in `audit_data(ignore=[…])`, with a
  separate call per nested object. Mandatory fields use one-arg `.pop("key")`, never
  `.get()`.
- No module-level constant used at only one site — inline it. Maps, headers and formats
  belong in the YAML (`/crawler-constants-to-yml`).
- Assertion bands come from the actual entity counts, not from the seat count.
- No `default_is_pep=True`, and no hand-rolled total-count guard.

Then run the output integrity checks in
`.claude/skills/crawler-pep/validation.md` — four qsv probes for dangling `Occupancy`
references, `role.pep` persons with no occupancy, and PEPs with no country. Each should
print nothing.
