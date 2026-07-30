---
description: New crawler ch_fiaa_freezes for Swiss FIAA asset freezes — frozen-CSV
  pattern (us_bis_mieu style) with a Fedlex SPARQL change monitor instead of Zyte/PDF-NER.
date: 2026-07-30
tags: [ch_fiaa_freezes, new-crawler, fedlex, frozen-csv, monitor, crawler-planning-590]
---

# ch_fiaa_freezes — Swiss asset freezes under the Foreign Illicit Assets Act

Source ask: https://www.eda.admin.ch/en/freezing-of-assets, tracked in
[crawler-planning#590](https://github.com/opensanctions/crawler-planning/issues/590),
which was closed as `blocked:not-a-list` / `feasibility:pdf-ner`.

## Why the old blockers no longer hold

- The EDA overview page 403s for bots (Zyte would be needed *only* for that page),
  but it is just prose — the names live in the annexes of Federal Council
  ordinances published on **fedlex.data.admin.ch**, which has no bot protection.
- Fedlex offers a SPARQL endpoint, RDF metadata per ordinance (in-force status,
  every dated consolidation), and Akoma Ntoso XML for recent versions. The two
  in-force annexes (Syria 2025, Venezuela 2026) are structured XML tables
  (Nr / Name / Geburtsdatum / Beschreibung). No PDF NER anywhere.
- Consequence: no Zyte, and the dataset `url` can point at the EDA page without
  ever fetching it.

## Scope: five FIAA-era ordinances (2016–present)

| Program | Ordinance (SR) | Fedlex ELI | Persons | Status |
|---|---|---|---|---|
| CH-FIAA-VE | Venezuela / Maduro (196.127.85) | cc/2026/1 | 37 | active |
| CH-FIAA-SY | Syria / al-Assad (196.127.27) | cc/2025/157 | 18 | active |
| CH-FIAA-UA | Ukraine / Yanukovych (196.127.67) | cc/2016/325 | 24 | expired 2023-02-27 → confiscation proceedings (`legacy`) |
| CH-FIAA-TN | Tunisia / Ben Ali (196.127.58) | cc/2016/324 | 48 | expired 2021-01-18 (`ended`) |
| CH-FIAA-EG | Egypt / Mubarak (196.123.21) | cc/2016/323 | 31 | lifted 2017-12-20 (`ended`) |

158 persons total. Pre-2016 constitutional ordinances (Duvalier, the 2011
Egypt/Tunisia and 2014 Ukraine predecessors) are out of scope — the 2016 FIAA
ordinances re-enacted them. Libya and the original Syria freeze folded into the
SECO/EmbA sanctions regime and are covered by ch_seco_sanctions.

## Design: frozen CSV + change monitor (us_bis_mieu pattern)

Decided with Friedrich on 2026-07-30 (over live XML parsing): annex changes are
rare (~1–3/year), the older annexes are prose that resists reliable parsing,
and legal-name fidelity favours human review of every change.

1. **`freezes.csv` in git** — one row per person per program: Name, Alias,
   Previous name, Birth date, Birth place, Nationality, Passport numbers,
   Notes (German, verbatim), Country, Program, Listed, Delisted, Source URL
   (the Fedlex consolidation where the person first appears).
2. **Per-person listing/delisting dates** reconstructed by diffing all 29
   consolidated versions across the five ordinances. Versions before ~2020
   only exist as PDF → pdftotext + hand-verified sequential diffs; Tunisia's
   two-column layout needed a dedicated parser. Scripts are throwaway
   (session scratchpad), not committed — the CSV is the reviewed artifact.
3. **Crawler** (`crawler.py`): reads the CSV, emits Person + Sanction with
   `program_key` from the CSV, start/end dates, `topics: sanction` gated on
   `h.is_active(sanction)` (55 active persons: SY+VE).
4. **Monitor** (`check_fedlex_versions`): one SPARQL query for every
   ConsolidationAbstract whose German title contains "Sperrung von
   Vermögenswerten", returning in-force status + all consolidation dates.
   Warnings on: unreviewed consolidation date, in-force flip, new ordinance,
   known ordinance disappearing. Reviewed state lives in the yml under
   `config.discovery.ordinances`, with a step-by-step runbook comment
   (mirrors the eCFR runbook in us_bis_mieu).
5. **Deploy**: `coverage.frequency: never` + daily `deploy.schedule` so the
   monitor runs; `manual_check` every 90 days to eyeball the EDA page in a
   browser (freezes under other titles/instruments, confiscation-proceeding
   news).

## Supporting metadata

- `meta/issuers/ch_fdfa.yml` — Federal Department of Foreign Affairs.
- `meta/programs/CH-FIAA-{VE,SY,UA,TN,EG}.yml` — modelled on the Canadian
  FACFOA programs (CA-FACFOA-TUN/UKR); statuses per the table above.

## Annex quirks handled

- Syria marks name variants in *italics*; the extractor turns them into
  aliases via an explicit hand-mapped table (e.g. "MAKHLOUF Hafiz *Hafez*" →
  alias "MAKHLOUF Hafez"); "xx.02.1972" → birth date "1972-02".
- Ukraine parentheticals ("Mykola (Nikolai) … AZAROV (geboren als …)") →
  alias / previousName.
- Egypt rows carry passport numbers from the annex text.
- Source typos kept verbatim ("MAHDOUl", "DHRlF" — present in DE *and* FR
  originals; no silent correction).

## Validation status (2026-07-30)

`zavod crawl` clean (316 entities, 0 issues); `zavod validate` passes;
`mypy --strict` passes; monitor negative-tested (removing a reviewed version
produces the expected warning).

## Open questions before merge

1. Naming: dataset `ch_fiaa_freezes` / prefix `ch-fiaa` / keys `CH-FIAA-*` —
   entity IDs hash the program key, so settle before release.
2. Ukraine program status `legacy` (assets still blocked via confiscation
   proceedings) vs `ended`.
3. Keep the "MAHDOUl" glyph verbatim or research the correct family name?
4. CSV is caught by the global `*.csv` gitignore → commit with `git add -f`.
