# Updating the Section 1286 list

Procedure for folding a new fiscal-year release of the DoD/DoW Section 1286 list
(foreign institutions engaging in problematic activities, FY19 NDAA §1286) into
the `us_special_leg` dataset. Follow this when a new FY list is announced —
roughly once a year. Last run: FY25, July 2026.

## Where the data lives

- The dataset is fed from a manually-maintained Google Sheet (edit URL in
  `us_special_leg.yml` `url:`, published CSV in `data.url`). There is no
  automated crawl of the PDF.
- Each fiscal year gets its **own complete set of rows** in the sheet, keyed by
  the `report-date` column (2022, 2023, 2024, …). A new release means adding a
  full new year-block, not editing old rows.
- Entity IDs are `make_slug(name)` (see `crawler.py`), so the same name across
  years merges into one entity with multiple sanctions. **A changed name splits
  the entity** — this drives most of the judgment calls below.
- The official PDFs have been hosted on `basicresearch.defense.gov` and
  `rt.cto.mil/wp-content/uploads/`. The press release usually precedes the URL
  being indexed by search engines.

## Procedure

1. **Get the PDF** and the current sheet CSV. Count the institutions the press
   release claims (e.g. FY25: "130 academic and research institutions") — this
   is the reconciliation target for top-level Table 1 entries.
2. **Extract the text twice**: `pdftotext -layout` for transcription, and
   pdfminer restricted to the English column (`LTChar.x0 < 430`) for
   verification. The layout interleaves the Native Name column into wrapped
   English names, and hyphenates across line breaks — never trust a single
   extraction, and never transcribe from the rendered page images alone.
3. **Transcribe Table 1 exactly as printed**, including the PDF's own typos
   (see gotchas). Main cell text becomes `name`; bullet points become
   `aliases`, joined with `"; "`. Strip trailing "and affiliates" /
   "and select affiliates:" from names. `schema=Company`, `topics=debarment`,
   `report-date=<year>`, `country` from the "Foreign Country of Concern"
   column, `program` = the exact string mapped to `US-MCCAIN-1286` in the yml
   lookup.
4. **Apply the sheet's structural conventions** (do not re-derive these):
   - *Academy of Military Medical Sciences*: parent row + one row per
     sub-institute, named `Academy of Military Medical Sciences, <X>`; any
     "(a.k.a. …)" goes in aliases.
   - *Chinese Academy of Sciences*: **no parent row** (listing says "select
     affiliates"); one row per affiliate, named
     `Chinese Academy of Sciences – <X>` (en dash).
   - *China Academy of Engineering Physics*: one row, all bulleted institutes
     as aliases.
   - *Table 2 talent programs*: one row each for the named programs; the
     generic "any other program meeting CHIPS Act §10638(4) criteria" row is
     never included.
5. **Build a delta CSV, not sheet rows directly**: previous-year rows vs the
   new list, with a trailing `change` column (`KEEP` / `ADD` / `REMOVE`).
   Script it, and make it assert:
   - top-level entry count == the press-release number;
   - every previous-year row is matched exactly once (KEEP, via an explicit
     old-name→new-name mapping) or explained as REMOVE — no leftovers;
   - every `name` and alias string appears verbatim in the English-column
     extraction (whitelist: sheet-constructed AMMS/CAS names, carried-over
     aliases, and wrap/hyphenation artifacts you have inspected individually).
   Normalize curly/straight apostrophes when matching — the sheet mixes them.
6. **Flag judgment calls in the `notes` column** and stop for maintainer
   review before touching the sheet:
   - *Renames*: when the new PDF prints a different English name for the same
     institution (translation drift is common: NCO↔Non-Commissioned Officer,
     College↔Academy, Defence↔Defense), record the old name in `notes`. The
     maintainer decides between PDF fidelity and slug continuity.
   - *Merges/supersessions*: an entry absorbed into another (FY25: Lomonosov
     MSU became an alias of Moscow State University) or replaced by a related
     entity (FY25: IHEP/ITEF out, NRC "Kurchatov Institute" in) — REMOVE plus
     cross-referencing notes on both sides.
7. **After the sheet is updated**, touch `us_special_leg.yml`:
   - add the new FY to the Section 1286 paragraph in `description`;
   - bump `manual_check.last_check`;
   - sanity-check `assertions` against the new Company entity count
     (each genuinely new institution and each accepted rename adds one);
   - fill the sheet's `source_url` for the new rows once the official PDF URL
     is live (leave empty until then).

## Gotchas (as of FY25)

- The PDF carries its own typos and preserves some across years — reproduce,
  don't fix: "Xi'an Jiatong University" (Jiaotong, since FY24),
  "Air Force Shinjianzhuang Flight Academy" (Shijiazhuang),
  "Changchung University of Science and Technology" (Changchun),
  "Hangzhao Dianzi University" (Hangzhou).
- Native-script names and per-entry countries were historically not captured
  in the sheet (`country` is populated from FY25 on).
- Extraction tools can disagree on marginal spacing ("P. I. Baranov" vs
  "P.I. Baranov") — check the rendered page when they do.
- "Formerly X" bullets are aliases, and usually the signal that a KEEP entry
  was renamed rather than a new institution added.
