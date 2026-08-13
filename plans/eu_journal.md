---
description: Current and proposed architecture for discovering, extracting, and retiring EU Official Journal sanctions designations.
date: 2026-08-13
tags: [eu-journal, sanctions, change-detection, cellar, celex, crawler-design]
---

# EU Journal sanctions change detection

## Why this exists

EU sanctions take effect when the legal act is published, while the Commission's
consolidated sources can lag behind. `eu_journal_sanctions` bridges that interval, but
its Google Sheet has become an operational database without the controls of one:

- amendments are found by a separate alerting service and handled manually;
- annexes are transcribed into a shared sheet, with limited provenance and reviewability;
- rows must be removed manually after they appear in FSF or another canonical feed;
- the dataset records the sanctions regime, but not the particular restrictive measure
  or annex under which an entity is listed; and
- discovery, extraction, consolidation checks, and publication each maintain a different
  partial model of EU legislation.

This plan describes the current system and sketches a git-managed replacement. It is a
transitional design document: unresolved points are intentionally recorded here until
the prototype produces evidence for the final crawler design.

The safety policy is deliberately asymmetric. Missing a new designation creates an
immediate coverage gap, so additions should be published from amendments as quickly as
possible. A lifted designation may remain present briefly while the consolidated source
catches up, but retaining it for longer creates legal liability. Consolidated parsing is
therefore not merely a second-source quality check: it is the mechanism that must remove
stale designations from the published dataset.

## Scope

The immediate target is a small shared module, `zavod.shed.ojeu`, plus a new prototype
dataset named `eu_journal`. The module should make CELEX handling, CELLAR retrieval, and
legal-act relationship lookup boring and reusable. The crawler can then detect
unreviewed acts and let the existing issues agent propose reviewed static CSV changes,
following the pattern demonstrated by the `cn_sanctions` extraction PRs.

This MVP should replace the mutable sheet with reviewed CSV files and make an
amendment's lifecycle explicit. Deterministic annex parsers and consolidated-regulation
parsers are later consumers of the same retrieval module, not MVP requirements.

This plan does not propose changing Watchful, `eu_fsf`, `eu_sanctions_map`, the FtM
schema, or the public coverage documentation yet. Those are follow-on scopes once the
prototype establishes which metadata and measure distinctions can be represented
reliably.

## Current architecture

```text
CELLAR metadata
    │
    ▼
Watchful SPARQL poll ──► Slack alert ──► analyst reviews EUR-Lex act
                                                │
                                                ▼
                                      manual annex transcription
                                                │
                                                ▼
                                     Google Sheet: Unconsolidated
                                                │
                                                ▼
                                 eu_journal_sanctions crawler
                                     │          │          │
                                     │          │          └─► name-in-consolidated-text warning
                                     │          └─► resolver check against canonical EU feeds
                                     └─► FtM entities + generic Sanction records
```

There is also a `Context` worksheet containing historical rows. It emits reduced
context-only entities and is not part of the retirement check.

### 1. Discovery and notification: Watchful

`operations/watchful/watchful/tasks/eu_journal.py` polls the CELLAR SPARQL endpoint
every 30 minutes and looks back five days. It selects:

- every `REG_IMPL` and `DEC_IMPL` classified under the PESC subject matter or linked to
  a known framework act; and
- `REG` and `DEC` acts whose English title contains one of three sanctions phrases.

The fallback framework list is a manually maintained tuple of CELEX codes. Each unseen
CELEX is persisted in `eu_journal_seen` and creates one Slack message. "Seen" only means
"alert posted": the state does not record whether an act was irrelevant, paired with
another act, extracted, reviewed, or later consolidated.

Strengths:

- CELLAR metadata is queried directly and deduplication is deterministic;
- the framework relationship catches some classifier failures; and
- alerts arrive independently of the slower FSF publication cycle.

Failure modes:

- a five-day rolling window is not a durable cursor and cannot recover a longer outage;
- the PESC classifier, resource types, title keywords, and framework allowlist are
  overlapping heuristics without a completeness measurement;
- every implementing act is treated as designation-bearing even when it is procedural,
  a correction, a delisting, or changes a non-entity annex;
- paired CFSP decisions and regulations create duplicate analyst work; and
- Slack reactions carry workflow state that is not available to the crawler.

### 2. Analyst workflow and extraction

The operational guide in `zavod/docs/eu_journal_sanctions.md` tells an analyst to claim
an alert in Slack, prefer a regulation when a decision/regulation pair represents the
same change, inspect the act, and paste relevant rows into the `Unconsolidated` and
`Context` tabs. A saved-HTML table extractor exists, while the newer `extract/` folder
is an exploratory data-entry aid. Neither is treated as a production parser here.

The live `Unconsolidated` sheet snapshot inspected on 2026-08-13 contains 703 rows from
34 distinct source URLs. Its 25 columns mix four concerns:

- identity and schema (`List ID`, `Type`, names, identifiers);
- legal provenance (`Source URL`, `startDate`);
- sanctions metadata (`Notes`, inferred program); and
- FtM-specific modelling (`related`, crypto wallets, schema-dependent fields).

The sheet has no explicit amendment CELEX column, framework/consolidated CELEX column,
annex identifier, operation (`add`, `modify`, `remove`), measure, review status, or
parser version. The source CELEX can only be recovered from the URL, and the program is
inferred at crawl time from the legal act's title.

### 3. Publication: `eu_journal_sanctions`

`datasets/eu/journal_sanctions/crawler.py` downloads both sheet tabs. For each active
row it:

1. extracts the source CELEX from the URL;
2. fetches the act from CELLAR and infers the amended framework number from its title;
3. maps that framework number to an OpenSanctions program key through dataset lookups;
4. emits an FtM entity and a generic `Sanction`; and
5. uses the entity resolver to warn if the entity is already represented by FSF or the
   Sanctions Map.

Rows resolved to FSF or Sanctions Map entities are collected into sheet ranges suggested
for manual deletion. Travel Ban matches are reported but do not trigger retirement.
Operationally, rows are also removed from the active sheet when their designation has
graduated into the canonical travel-ban coverage; the crawler's report-only handling of
Travel Ban matches does not fully encode that intended lifecycle.

For designations older than 90 days, the crawler also follows CELLAR's `amends`
relationship to the latest consolidated framework act and searches its normalized full
text for each source name. This detects some stale rows, but it is not a structured
parity check: a name can occur outside the operative annex, spelling changes require
lookups, and absence cannot distinguish delisting from parsing or consolidation lag.

### 4. Canonical EU sources and measure ambiguity

The three sources overlap but do not have the same scope:

- `eu_fsf` is the Commission's consolidated list of targets subject to financial
  sanctions: asset freeze plus the prohibition on making funds or economic resources
  available.
- `eu_sanctions_map` includes named targets of other restrictive measures, including
  entities and vessels that are not asset-frozen.
- `eu_travel_bans` represents a different measure and is not proof of FSF consolidation.

The current journal crawler only preserves the regime/program. A target listed under an
asset freeze, travel ban, sectoral transaction restriction, vessel port-access measure,
or several measures receives the same generic representation. As a result, "present in
another EU feed" is not sufficient to prove that the same legal designation and measure
have been consolidated.

### 5. Documentation drift

`knowledgebase/site/content/docs/coverage.md` describes discovery as starting with the
Sanctions Map API and using web scraping plus the EUR-Lex SOAP service. That does not
match the current Watchful implementation, which uses CELLAR SPARQL and a fixed framework
allowlist. Public documentation should be updated only after the replacement workflow is
implemented and its completeness is measured.

## Proposed architecture

```text
CELLAR discovery ledger
    │
    ├─► classify act + pair related acts + record review outcome
    │
    ▼
deterministic document fetch (prefer Formex/XML; XHTML fallback)
    │
    ├─► extract legal changes and annex rows
    ├─► validate against checked-in fixtures
    └─► analyst review of generated CSV diff
                    │
                    ▼
       data/amendments/{CELEX}.csv
                    │
                    ▼ framework receives a new consolidation
       parse full operative annex and compare by measure
                    │
                    ├─► data/consolidated/{CONSOLIDATED_CELEX}.csv
                    └─► block amendment CELEX in dataset YAML
```

The git repository becomes both the review surface and the workflow ledger. Source
documents remain authoritative; CSVs are deterministic, reproducible derived artifacts.

## MVP: shared OJEU harness plus agent-reviewed CSVs

The first version should not attempt universal deterministic annex extraction. It should
provide reliable legal-source plumbing and an issue shape that a maintenance agent can
act on:

```text
zavod.shed.ojeu
    ├─ normalize CELEX and construct canonical URLs
    ├─ fetch CELLAR metadata and document expressions
    └─ resolve amended frameworks and consolidated versions
             │
             ▼
eu_journal crawler discovers an unreviewed act
             │
             ▼
actionable issue: CELEX + title + relationships + source URLs
             │
             ▼
issues agent inspects official text and proposes data/amendments/{CELEX}.csv
             │
             ▼
human reviews the source evidence and CSV diff
```

This matches the agent-assisted static-data pattern used in OpenSanctions PR 5363: the
crawler detects an unreviewed official notice, while the agent changes only reviewed
dataset-owned CSV data. The agent is not a runtime parser and its proposal is not
published without normal PR review.

### `zavod.shed.ojeu` responsibilities

Keep the module small and source-oriented:

- parse and validate CELEX values from raw values, EUR-Lex URLs, and ELI URLs;
- construct canonical EUR-Lex, ELI, and CELLAR resource URLs;
- fetch a selected language expression with explicit content negotiation and return the
  source bytes, media type, final URL, and content hash;
- retrieve basic act metadata needed for review: title, document date, resource type,
  amended/based-on frameworks, and available consolidated CELEX versions; and
- select the latest consolidated version using explicit metadata rather than title or
  filename guessing.

The same operations need two usable surfaces:

- `Context`-aware functions for crawlers, using zavod caching, resource export, logging,
  and URL invalidation; and
- executable package modules for the issues agent and humans, producing inspectable
  files or JSON without requiring a dataset crawl or Watchful database. The commands
  are directly invokable as:

  ```bash
  python -m zavod.shed.ojeu.celex 32026R1708
  python -m zavod.shed.ojeu.cellar 32026R1708 --output /tmp/32026R1708.xhtml
  python -m zavod.shed.ojeu.cellar 32026R1708 \
    --body-only --output /tmp/32026R1708.html
  ```

Avoid implementing two HTTP clients with different semantics. The standalone surface
should be a thin adapter over the same URL, content-negotiation, parsing, and result
types used by the crawler functions.

A tentative package shape is:

```text
zavod/shed/ojeu/
  __init__.py
  celex.py       # CELEX normalization, metadata/relationship lookup, executable entry
  cellar.py      # CELLAR content negotiation and expression fetching
```

`celex.py` should accept either a bare CELEX or a supported EUR-Lex/ELI URL. Its default
output should be stable, machine-readable JSON containing the normalized CELEX,
canonical URLs, title/date/type, framework relationships, consolidated versions, and
the selected expression metadata. Optional flags may save the source expression to a
specified path; avoid writing into the repository implicitly.

`cellar.py` exposes the official expression bytes independently of the metadata command.
By default it writes the content-negotiated expression to stdout or an explicit output
path. `--body-only` emits a deterministic HTML derivative containing the act body and
its annex tables, without the XML declaration, document head, or EUR-Lex presentation
chrome. The raw CELLAR expression remains the provenance artifact; the body derivative
is a workspace for human or agent-assisted table extraction.

### Explicit non-responsibilities

The shared module should not:

- extract FtM entities or write amendment CSVs;
- decide whether an act contains additions, modifications, or removals;
- classify detailed legal measures;
- map an act to an OpenSanctions program key;
- own reviewed/unreviewed workflow state; or
- decide whether an amendment is safe to block.

Those decisions belong to the dataset, the issues-agent task, and human review. Keeping
them out of `zavod.shed` prevents a generic legal-source client from becoming an implicit
EU sanctions policy engine.

### MVP measure model

Measure classification should remain intentionally coarse. The first CSV contract only
needs to distinguish the handoff destinations that affect lifecycle, for example:

- `financial` — expected to consolidate into FSF;
- `travel` — expected to consolidate into the travel-ban source;
- `other` — named restrictive measures covered elsewhere or only by the legal act; and
- `unknown` — requires review before handoff.

This is routing metadata, not a comprehensive legal taxonomy. More detail should only be
added in response to a demonstrated publication or retirement requirement.

### Actionable issue contract

An unreviewed-act warning should give the issues agent enough evidence to begin without
rediscovering the document:

- source CELEX and canonical official URL;
- English title, document date, and resource type;
- amended/based-on framework CELEXes;
- latest consolidated CELEX, if one exists;
- exported or directly fetchable official expression and its media type/hash; and
- the exact reviewed-state key or missing expected CSV path.

The warning should not claim that designations exist. Its meaning is: this relevant act
has not yet received a recorded human outcome. A valid agent PR may add amendment rows or
record that the act is paired, irrelevant, contains only removals, or needs follow-up.

### Repository layout

Tentative layout under `datasets/eu/journal/`:

```text
eu_journal.yml
crawler.py
data/
  amendments/
    32026R1708.csv
  consolidated/
    02024R1485-20260713.csv
fixtures/
  documents/                 # small, explicitly selected parser fixtures
```

The exact fixture policy should be decided before checking in source documents; full EU
document snapshots may be unnecessarily large. At minimum, tests need immutable source
content hashes and enough representative snippets to run without the network.

### CSV contract

Use the same entity column contract in both folders so the crawler can concatenate them.
Every row should include explicit legal metadata rather than deriving it from display
text at runtime:

- `source_celex`: the act from which this row was extracted;
- `framework_celex`: the original regulation or decision being amended;
- `annex`: stable annex/section identifier where available;
- `measure`: coarse routing value such as `financial`, `travel`, `other`, or `unknown`;
- `operation`: `add`, `modify`, or `snapshot`;
- `effective_date`;
- `program_key`;
- `record_id`: a stable legal-record identifier within the act/annex; and
- the reviewed entity fields needed to emit FtM records.

`source_celex + annex + record_id + measure` should be unique within a file. Entity IDs
must not depend on a CSV row number or the entity's current spelling. Where the legal act
does not provide a durable record identifier, define and test a fingerprint from stable
source fields and preserve an explicit override mechanism.

Avoid encoding workflow state in the CSV rows. File presence, review through git, and the
dataset lookup should determine whether a source is active.

Amendment CSVs are a fast positive-change feed. They need to represent additions and
modifications, but do not need to apply delistings directly. A removal act should still
be discovered, classified, and linked to its framework in the ledger; it does not create
a negative entity row or delete an amendment CSV. The removal becomes effective in this
dataset when a reviewed consolidated snapshot no longer contains the legal record.

This avoids implementing legal patch semantics twice. It also makes consolidated
freshness safety-critical: monitoring must alert when a known removal has not appeared
in a consolidated version within the accepted grace period.

### Amendment lifecycle

1. Discovery records a CELEX and its relationships, even if it is later classified as
   irrelevant or paired with another act.
2. The extractor produces a candidate `data/amendments/{CELEX}.csv`.
3. An analyst reviews the document, measure classification, parser output, and git diff.
4. The checked-in amendment becomes active dataset input.
5. When CELLAR publishes a new consolidated version of the framework, the consolidated
   parser writes a full `data/consolidated/{CONSOLIDATED_CELEX}.csv` snapshot.
6. A structured comparison proves which amendment records and measures are represented
   in the snapshot, and which records have disappeared since the prior snapshot.
7. The amendment CELEX is added to a blocking lookup in `eu_journal.yml`; its CSV remains
   in git for provenance but the crawler no longer emits it.
8. Only the latest reviewed consolidated snapshot for each framework and measure is
   active. When a designation is absent from that snapshot, it stops being emitted;
   older snapshots remain in git but never keep the entity sanctioned.

A tentative lookup shape is:

```yaml
lookups:
  block.amendments:
    options:
      - match: 32026R1708
        value: 02024R1485-20260713
```

The value records the consolidating snapshot rather than a boolean, making the handoff
auditable. The loader must fail if a blocked amendment names a missing consolidated CSV,
if a file's CELEX disagrees with its contents, or if two active snapshots cover the same
framework and measure without an explicit policy.

Do not block an amendment merely because the same entity resolves to an FSF entity. The
handoff condition is that the same legal record and measure are present in an identified
consolidated source. A discovered removal does not block or delete data on its own; the
reviewed consolidated snapshot applies the removal.

### Discovery ledger

The new design needs more state than Watchful's set of seen CELEXes. Whether the ledger
lives in Watchful's database or as git-managed metadata should be decided after the
prototype, but its model should include:

- CELEX, document date, resource type, title, and canonical URL;
- amended/based-on framework CELEXes from CELLAR;
- related or paired acts;
- discovery time and source query/cursor;
- classification: designation change, delisting, modification, non-entity measure,
  corrigendum, irrelevant, or unknown;
- extraction/review status and resulting CSV path; and
- latest known consolidated CELEX for each framework, plus any pending removal awaiting
  consolidation.

Discovery should use an overlap window for resilience, but correctness should come from
a durable high-water mark plus backfill/audit queries, not from assuming every outage is
shorter than the lookback. The known-framework registry should be generated or reconciled
against legal relationships and the Sanctions Map rather than copied independently into
Watchful and the dataset YAML.

## Feasibility of deterministic annex extraction

The ambition is feasible, but not as a single universal table scraper.

EUR-Lex and CELLAR expose structured metadata, legal relationships, and several content
formats. The Publications Office documents direct CELLAR access and notes that many
Official Journal documents are available as Formex XML in addition to HTML/XHTML and
PDF. Consolidated acts also have distinct, date-suffixed CELEX identifiers. These are
strong foundations for deterministic fetching, version discovery, and provenance:

- [Reuse EUR-Lex content](https://eur-lex.europa.eu/content/help/data-reuse/reuse-contents-eurlex-details.html)
- [CELLAR documentation](https://op.europa.eu/en/web/cellar/documentation)
- [EUR-Lex permanent link formats](https://eur-lex.europa.eu/content/help/data-reuse/linking.html)

The hard part is legal structure, not transport. Restrictive regimes use different annex
layouts, headings, numbering schemes, entity categories, and amendment language. Some
acts replace whole annexes; others add, replace, renumber, or delete individual entries.
The same person may be subject to different measures through a CFSP decision and a
regulation. Consolidated texts are documentation tools without legal effect, so original
Official Journal acts must remain the provenance for effective changes even if the
consolidated text is used as a full-state cross-check.

Expected feasibility by layer:

| Layer | Feasibility | Qualification |
|---|---|---|
| CELEX discovery and relationship graph | High | CELLAR metadata is structured; completeness still needs a backtest. |
| Fetching a stable language/version | High | Prefer Formex/XML, retain XHTML fallback and content hashes. |
| Identifying changed annexes and operation | Medium | Requires legal-pattern parsing and regime-specific fixtures. |
| Extracting rows from known annex families | Medium to high | Deterministic adapters can cover recurring layouts. |
| Normalizing every field into FtM | Medium | Names and identifiers are tractable; free-text addresses, reasons, and relations need review. |
| Classifying the exact restrictive measure | Medium | Often derivable from the framework article/annex, but it needs a controlled registry and legal review. |
| Universal parser for all restrictive acts | Low initially | Build coverage by annex family and fail closed on unknown structures. |

The safest route is a parser registry keyed by framework CELEX and annex family, backed
by golden fixtures. A generic Formex/XHTML layer should preserve document structure;
small regime adapters should declare which annexes contain targets, which measures they
represent, and how row fields map to the CSV contract. Unknown or changed layouts must
produce a reviewable failure, never an empty successful extraction.

## Correctness invariants

- Every emitted row points to an immutable source CELEX and a checked-in CSV.
- A parser run is reproducible from the same source bytes and parser version.
- An empty annex extraction is only accepted when a fixture explicitly expects it.
- File naming, embedded CELEX values, and CELLAR metadata agree.
- Paired decision/regulation acts are related explicitly; deduplication never depends on
  title similarity alone.
- Measure is explicit and never inferred from mere presence in FSF or Sanctions Map.
- Blocking an amendment requires an identified consolidated version and a successful
  structured parity check for the relevant legal records/measures.
- Amendment parsing may defer removals, but every discovered removal remains pending
  until a reviewed consolidated snapshot proves the resulting current state.
- Only the latest active consolidated snapshot can emit a framework/measure. An entity
  absent from it cannot be kept active by an older snapshot or amendment.
- Consolidation lag for a known removal is measured and alerted against an explicit
  maximum grace period; stale sanctions cannot age silently.
- Amendment and consolidated inputs cannot silently emit the same legal record twice.
- Parser changes are run against all fixtures and produce reviewed CSV diffs.
- Network discovery can fail without deleting or changing the last reviewed dataset.

## Proposed checkpoints

### Checkpoint 1: shared CELEX/CELLAR harness

- Define the minimal result types and public functions in `zavod.shed.ojeu`.
- Add fixture tests for CELEX normalization, URL variants, content negotiation, framework
  relationships, and consolidated-version selection.
- Add `python -m zavod.shed.ojeu.celex CELEX` and
  `python -m zavod.shed.ojeu.cellar CELEX`, usable by humans and the issues agent without
  a dataset crawl.
- Convert the current journal crawler's duplicated CELLAR helpers to the shared module
  only after the new interface is tested.

Deliverable: a reusable, documented fetch harness. It must not contain entity extraction
or sanctions-policy decisions.

### Checkpoint 2: git-managed amendment and issues-agent prototype

- Define and validate a deliberately small CSV schema.
- Implement the new `eu_journal` loader for checked-in amendment files only.
- Emit an actionable issue for a relevant CELEX with no reviewed outcome.
- Give the issues-agent prompt the OJEU investigation command and static-data rules.
- Exercise the full flow on a small representative set: warning, agent-proposed CSV,
  human-reviewed PR, and clean subsequent crawl.

Deliverable: one end-to-end agent-assisted extraction PR. The measurement is whether the
agent found the correct official text and proposed source-supported CSV rows, not whether
the process was fully automatic.

### Checkpoint 3: inventory and discovery backtest

- Build a canonical registry of relevant framework acts, annexes, and measures.
- Query historical CELLAR metadata for a bounded period and compare results with the
  current sheet sources, Watchful alerts, and known sanctions packages.
- Categorize false positives and false negatives before changing Watchful.
- Select a fixture matrix spanning regulations/decisions, add/modify/remove operations,
  full-annex replacements, corrigenda, and non-FSF measures.

Deliverable: measured discovery recall/precision and a reviewed fixture list. Any missed
known act is a correctness failure and should stop the work before extractor design.

### Checkpoint 4: optional deterministic amendment adapters

- Implement the common Formex/XHTML document layer.
- Add framework/annex-specific adapters for the fixture set.
- Generate candidate CSVs and require clean golden-file diffs.
- Record explicit review outcomes for acts with no entity changes.

Deliverable: measured extraction coverage and field-level parity by act family.

### Checkpoint 5: consolidated snapshots and handoff

- Resolve the latest consolidated CELEX per framework.
- Parse full operative annex snapshots using the same row contract.
- Compare amendment operations to snapshot state by legal record and measure.
- Implement and validate `block.amendments`.
- Include a delisting fixture and prove that the new snapshot stops emitting the removed
  designation while retaining it in historical git artifacts.

Deliverable: a demonstrated amendment-to-consolidated transition for several regimes,
including one non-FSF measure. Do not automate blocking until mismatches have been
reviewed and the comparison semantics are stable.

### Checkpoint 6: operational integration

- Decide whether the discovery ledger belongs in Watchful or the dataset repository.
- Replace Slack-only workflow state with ledger-backed status and alerts.
- Migrate the remaining active sheet rows with explicit provenance and measures.
- Run both datasets in parallel for an agreed observation period.
- Update the operational guide and public coverage page to describe the implemented
  system, then retire the Google Sheet crawler.

## Open questions

1. Is one CSV row a legal annex entry, an FtM entity, or an entity-measure pair? The
   entity-measure pair is the most explicit but may repeat identity fields.
2. Are the four coarse routing values (`financial`, `travel`, `other`, `unknown`)
   sufficient to decide publication and retirement, or can the MVP omit `measure`
   entirely and derive the expected handoff from the framework registry?
3. Should consolidated files be complete snapshots per consolidated CELEX, or one file
   per framework/annex/measure? CELEX-only filenames favour the former, while large
   multi-annex regulations may favour a manifest plus several files.
4. How should modifications retain identity when the legal entry number changes or the
   consolidated text renumbers an annex?
5. Which act is primary when a CFSP decision and regulation differ in fields, timing, or
   measure scope? "Prefer the regulation" is an operational shortcut, not a general
   provenance rule.
6. Can all current sheet rows be assigned an amendment CELEX, framework CELEX, annex,
   and measure without legal re-review?
7. Is the desired second-source comparison against FSF entity records, against the legal
   framework's complete asset-freeze annex, or both? These answer different correctness
   questions.
8. What is the maximum acceptable interval between a removal act and the consolidated
   snapshot applying it? This should become an operational alert/SLO, not an implicit
   assumption.

## Immediate next decision

Before implementing the dataset, settle the minimal public interface of
`zavod.shed.ojeu` and exercise it against one recent amendment plus its consolidated
framework. The first vertical slice should prove only that:

- the crawler and a standalone investigation command resolve the same CELEX metadata
  and source bytes;
- the crawler emits a self-contained unreviewed-act issue; and
- an issues-agent run can use that evidence to propose a CELEX-named CSV for human
  review, following the PR 5363 pattern.

If that works, broaden the discovery fixture set. Do not make deterministic entity
extraction or a detailed measure taxonomy prerequisites for the first agent-assisted PR.
