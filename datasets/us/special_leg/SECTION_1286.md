# Updating the Section 1286 list

Procedure for folding a new fiscal-year release of the DoD/DoW Section 1286 list
(foreign institutions engaging in problematic activities, FY19 NDAA §1286) into
the `us_special_leg` dataset. Follow this when a new FY list is announced —
roughly once a year. Last run: FY25, July 2026.

## Where the data lives

- The Section 1286 rows live in `source_files/section_1286.csv`, maintained by
  pull request. There is no automated crawl of the PDF.
- Each fiscal year gets its **own complete set of rows**, keyed by the
  `report-date` column (2022, 2023, 2024, …). A new release means adding a
  full new year-block, not editing old rows.
- Entity IDs are `make_slug(name)` (see `crawler.py`), so the same name across
  years merges into one entity with multiple sanctions. **A changed name splits
  the entity** — this drives most of the judgment calls below.
- The official PDFs have been hosted on `basicresearch.defense.gov` and
  `rt.cto.mil/wp-content/uploads/`. The press release usually precedes the URL
  being indexed by search engines.

## Procedure

1. **Get the PDF** and the current `source_files/section_1286.csv`. Count the
   institutions the press release claims (e.g. FY25: "130 academic and research
   institutions") — this is the reconciliation target for top-level Table 1
   entries.
2. **Extract the text twice**: `pdftotext -layout` for transcription, and
   pdfminer restricted to the English column (`LTChar.x0 < 430`) for
   verification. The layout interleaves the Native Name column into wrapped
   English names, and hyphenates across line breaks — never trust a single
   extraction, and never transcribe from the rendered page images alone.
3. **Transcribe Table 1 exactly as printed**, including the PDF's own typos
   (see gotchas). Main cell text becomes `name`; bullet points become
   `aliases`, joined with `"; "` — one row per entity, never one row per alias
   (the FY22 block was transcribed that way and has been collapsed; the crawler
   now rejects a name repeated within an edition). Strip trailing
   "and affiliates" / "and select affiliates:" from names.
   `schema=Company`, `topics=debarment`,
   `report-date=<year>`, `country` from the "Foreign Country of Concern"
   column, `program` = the exact string mapped to `US-MCCAIN-1286` in the yml
   lookup.
4. **Apply the file's structural conventions** (do not re-derive these):
   - *Academy of Military Medical Sciences*: parent row + one row per
     sub-institute, named `Academy of Military Medical Sciences, <X>`; any
     "(a.k.a. …)" goes in aliases.
   - *Chinese Academy of Sciences*: **no parent row** (listing says "select
     affiliates"); one row per affiliate, named
     `Chinese Academy of Sciences - <X>` (ASCII hyphen). The separator and the
     `Chinese Academy of Sciences` prefix are ours, not the PDF's — the list
     prints only the bullet. Keep the affiliate name exactly as bulleted; do
     not add acronyms the PDF does not print (`(CAS)`, `(ICT)`), which split
     the entity across years. Acronyms belong in `aliases`.
   - *China Academy of Engineering Physics*: one row, all bulleted institutes
     as aliases.
   - *Table 2 talent programs*: **not imported at all**. Table 2 lists
     foreign talent recruitment programmes (Thousand Talents Plan, Project
     5-100, …), which are funding and recruitment schemes rather than
     institutions, so there is no entity to screen and no FollowTheMoney
     schema that fits. They were imported as `Company` until FY25 and have
     been removed. Only Table 1, "List of Institutions", is transcribed.
5. **Build a delta CSV, not `section_1286.csv` rows directly**: previous-year
   rows vs the new list, with a trailing `change` column
   (`KEEP` / `ADD` / `REMOVE`).
   Script it, and make it assert:
   - top-level entry count == the press-release number;
   - every previous-year row is matched exactly once (KEEP, via an explicit
     old-name→new-name mapping) or explained as REMOVE — no leftovers;
   - every `name` and alias string appears verbatim in the English-column
     extraction (whitelist: locally constructed AMMS/CAS names, carried-over
     aliases, and wrap/hyphenation artifacts you have inspected individually).
   Normalize curly/straight apostrophes when matching — the file mixes them.
6. **Flag judgment calls in the `notes` column** and stop for maintainer
   review before touching `section_1286.csv`:
   - *Renames*: when the new PDF prints a different English name for the same
     institution (translation drift is common: NCO↔Non-Commissioned Officer,
     College↔Academy, Defence↔Defense), record the old name in `notes`. The
     maintainer decides between PDF fidelity and slug continuity.
   - *Merges/supersessions*: an entry absorbed into another (FY25: Lomonosov
     MSU became an alias of Moscow State University) or replaced by a related
     entity (FY25: IHEP/ITEF out, NRC "Kurchatov Institute" in) — REMOVE plus
     cross-referencing notes on both sides.
7. **After `section_1286.csv` is updated**, touch `us_special_leg.yml`:
   - add the new FY to the Section 1286 paragraph in `description`;
   - bump `manual_check.last_check`;
   - sanity-check `assertions` against the new Company entity count
     (each genuinely new institution and each accepted rename adds one);
   - fill the new rows' `source_url` once the official PDF URL is live
     (leave empty until then).

## Gotchas (as of FY25)

- The PDF carries its own typos and preserves some across years — reproduce,
  don't fix: "Xi'an Jiatong University" (Jiaotong, since FY24),
  "Air Force Shinjianzhuang Flight Academy" (Shijiazhuang),
  "Changchung University of Science and Technology" (Changchun),
  "Hangzhao Dianzi University" (Hangzhou).
- Native-script names and per-entry countries were historically not captured
  (`country` is populated from FY25 on).
- Extraction tools can disagree on marginal spacing ("P. I. Baranov" vs
  "P.I. Baranov") — check the rendered page when they do.
- "Formerly X" bullets are aliases, and usually the signal that a KEEP entry
  was renamed rather than a new institution added.
- The em dash in `Tactical Missile Corporation, Concern “MPO—Gidropribor”`
  is printed by the PDF (the Russian column prints one too) — leave it. Only the
  separator in the constructed `Chinese Academy of Sciences - <X>` names is ours
  to normalise.
