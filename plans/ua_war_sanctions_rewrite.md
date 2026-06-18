---
description: Rewrite of the ua_war_sanctions crawler — drop the over-engineered API-dispatch
  abstraction, move toward public-website sourcing, and adopt IMO-based entity IDs.
date: 2026-06-07
tags: [ua_war_sanctions, crawler-rewrite, zyte, entity-ids, audit]
---

# ua_war_sanctions rewrite

## Why this exists

Three motivations converged:

1. **Staleness bug.** The private API serves manager assignments that lag the public
   website by ~a year. Concrete case — vessel 869 (PASIPHAE / ex-PACKARD, IMO 9289518):

   | Role | Private API (what the crawler emits) | Public website | API date |
   |---|---|---|---|
   | owner | Harborline Global Maritime Ltd (IMO 0178274) | Elyrion Navarc Corp (IMO 0381931) | 10.03.2025 |
   | commerce_manager | Namor Shipping DMCC (IMO 0182210) | Elyrion Navarc Corp | 10.03.2025 |
   | security_manager | Silverfin Marine LLP (IMO 0090524) | Zeythra Group LLC-FZ (IMO 0435281) | 12.03.2025 |

   The API record's narrative (`info`) *is* current (mentions Feb 2026 events), but the
   structured manager fields are not. The crawler faithfully reproduces stale data. The
   public page is the more current source.

2. **Legal preference.** Ingesting only publicly-published data is cleaner than depending
   on a confidentially-shared private API.

3. **Over-engineering.** The current crawler builds a `WSAPILink` frozen-dataclass +
   `WSAPIDataType` enum + a single generic dispatch loop. Preference is the "stupid but
   explicit" style of `datasets/_global/gleif/crawler.py`: plain dicts/tuples for config,
   one explicit parse function per record type, a flat `crawl()` that names each step.

## Audit findings (2026-06-07)

Bounded audit against both sources (private API via creds, public site via Zyte). Debug
scripts live in `contrib/debug_ws_*.py`.

### Public site is fetchable past bot protection
- Zyte `httpResponseBody` (no JS) returns full server-rendered pages. Browser rendering not
  needed. No Cloudflare/"just a moment" wall.

### No public JSON API — enumeration is HTML pagination
- Listing pages use Yii2 pagination: `/en/transport/ships?page=N&per-page=12`.
- **`per-page` is capped server-side at 12** (tested 100, 500 — still 12). Last ships page
  is 117 → ~1,404 vessels at 12/page.
- Enumeration therefore = paginate listings + scrape detail links. No JSON shortcut.

### IDs align across API and public site
- Vessel API id `869` ↔ `/transport/ships/869`.
- Company API id `14433` ↔ `/transport/ships-company/14433`.
- This is load-bearing: it lets the hybrid swap sources without changing IDs, and lets
  `rekey` migrate cleanly.

### Public URL patterns (from nav + ship-page links)
Nearly every API section has a public equivalent:
`/en/transport/ships`, `/transport/ships/{id}`, `/transport/ships-company/{id}` (maritime
companies — NOT `/transport/companies`), `/transport/captains`, `/transport/shadow-fleet`,
`/sanctions/companies`, `/sanctions/persons`, `/kidnappers/persons`, `/propaganda/persons`,
`/sport/persons`, `/executives`, `/rostec`, `/components/companies`, `/components/weapon`,
`/tools`, `/stolen`, `/uav/companies`. Language variants: `/en/`, `/ru/`, and bare (uk) —
covers `name_en` / `name_ru` / `name_uk`.

### Section counts (API)
- transport/ships: **1404**
- transport/management: **2212** (owner/commerce/security manager orgs)
- transport/companies (= public ships-company): **96**
- transport/persons: **11**
- transport/captains: **534**
- (other sections not yet counted)

### IMO coverage — IMO microformat is viable
- ships: 1402/1404 have an IMO; 1400 are clean 7-digit; **0 collisions**.
- management: 2190/2212 have an IMO; 2189 clean; **1 collision** (IMO `6327601` shared by 2
  records — must handle, not silently merge).
- Fallback needed for ~2 vessels and ~22 managers lacking a valid IMO.

### Field source-of-truth matrix (sampled)
Heuristic: is the API field's value visible on the public page? (`flag`/`country` show as
labels e.g. "Barbados"/"Hong Kong", not codes `BRB`/`HKG` — representation difference, not
absence.)

**Vessel 869** — nearly all substantive fields public (name, imo, mmsi, callsign, dwt,
weight, year, type, length, flag, previous names/flags, shadow group, ports, P&I club, links,
description). The **manager assignments diverge**: API's `security_manager` (Silverfin) is
absent from the page; the page shows the current managers instead.

**Company 14433** (at the correct `ships-company` URL) — `reg` ✓, `itn` ✓, name ✓, country ✓,
address ✓ all publicly visible. Only company `imo` not shown (minor). **The structured
identifiers we feared losing survive publicly.**

> Sampling caveat: one vessel + one company so far. A broader automated API-vs-public diff
> across all sections should run as a pre-cutover gate (see Open Questions).

### Name languages are per-path (fetch multiplier)
The three name forms live on separate language paths, not one page (person 23038):
- `/en/propaganda/persons/23038` → "ANTONOVSKII Roman Yuryevich" (Latin)
- `/ru/propaganda/persons/23038` → "АНТОНОВСКИЙ Роман Юрьевич"
- `/propaganda/persons/23038` (uk) → "АНТОНОВСЬКИЙ Роман Юрійович"

So `name_en` + `name_ru` + `name_uk` ⇒ **3× detail fetches** for persons/orgs with multilingual
names. Vessels are mostly Latin-named (likely 1×). Open question: is one English transliteration
+ the original-language form enough, or do we keep all three (and pay 3×)?

## Decision (2026-06-07): Model B — public-only

Drop the private API entirely. Enumerate + parse public pages; no confidential creds, no
token logic. Accept the fetch-volume and HTML-fragility costs. This honors the legal
preference and removes the staleness bug (public is the more current source).

## Implications

- **Public-only (Model B) is viable on content grounds.** Public pages carry every
  load-bearing field including reg/INN identifiers, and are *more current* than the API for
  managers. The only API-only fields seen are minor (company `imo`, internal numeric ids we
  don't emit anyway).
- **The cost is fetch volume.** Public-only means enumerating via ~117+ listing pages and
  fetching ~1,404 ship + 96 company + other-section detail pages every run — order of a few
  thousand Zyte requests/run, vs ~20 batched API calls today. Daily frequency may need to
  relax, or detail pages cached/conditionally re-fetched.
- **HTML fragility** replaces a stable JSON contract — more maintenance, needs defensive
  parsing (`h.xpath_element(s)`, fail-loud on layout drift).

## Entity ID scheme — IMO microformat

- `imo-v-9289518` — vessels (7-digit ship IMO).
- `imo-o-0090524` — ship-management orgs / P&I clubs (7-digit IMO *company* number).
- Normalization: bare digits, no `IMO` prefix, leading zeros preserved.
- **Scope is maritime only.** Non-maritime sections (kidnappers, athletes, propaganda,
  executives, rostec, components, tools, stolen, partner sanctions) have no IMO → keep their
  current reg/tax/API-id-based scheme. Persons → unchanged.
- **Fallbacks:** maritime entities without a valid IMO keep the existing API-id slug.
- **Collisions:** fail loud on duplicate IMO (the 1 known management collision).
- **Migration:** `context.rekey(old_id, new_id)` records a POSITIVE resolver judgement so the
  downstream `NK-` cluster and manual judgements follow the entity. Caveat: rekey **no-ops on
  the SQLite resolver** — only effective in a real (Postgres-resolver) run.

## Proposed crawler structure (GLEIF style)

Replace `LINKS` / `WSAPILink` / `WSAPIDataType` / generic dispatch with explicit steps:

```python
def crawl(context):
    crawl_vessels(context)        # ships + managers + ships-company + persons + captains
    crawl_kidnappers(context)
    crawl_athletes(context)
    crawl_propagandists(context)
    crawl_executives(context)
    crawl_stealers(context)
    crawl_rostec(context)
    crawl_components(context)
    crawl_partner_sanctions(context)
```

Each function fetches its source(s) and parses inline. The existing `crawl_vessel` /
`crawl_person` / `crawl_legal_entity` parse bodies are mostly reusable; it's the abstraction
layer above them that goes. Simple ID helpers: `vessel_id(imo)`, `org_id(imo)`.

## Implementation roadmap (Model B)

Phased, with checkpoints between phases.

1. **Per-section enumeration map.** For each public section, record listing URL, pagination
   shape, and detail URL pattern. Build one reusable `paginate(context, listing_url)` that
   yields detail URLs (handles the 12/page cap + last-page detection). Sections:
   ships, ships-company, captains, shadow-fleet, sanctions/{persons,companies},
   kidnappers/persons, propaganda/persons, sport/persons, executives, rostec, components/*,
   tools, stolen, uav/companies.
2. **Page parsers, one per page type.** Defensive lxml + `h.xpath_element(s)`; fail loud on
   layout drift. Reuse the existing `crawl_vessel`/`crawl_person`/`crawl_legal_entity` field
   mapping where the data is the same; the new work is HTML extraction, not FtM shaping.
3. **IMO IDs + rekey.** `vessel_id(imo)` / `org_id(imo)` helpers; fallback to API-id slug for
   no-IMO maritime entities; fail loud on IMO collision. Emit `rekey(old, new)` for every
   maritime entity whose ID changes.
4. **Name-language strategy** (pending decision below): 1× vs 3× fetch.
5. **Cadence + caching:** relax `frequency` from daily; use `cache_days` so re-runs reuse
   pages; consider conditional re-fetch.
6. **Pre-cutover diff gate:** broaden the API-vs-public diff to 3–5 entities per section,
   produce an explicit gains/losses changelog, get a second review, then hard-replace
   `crawler.py`.

## Progress

- **Vessel page parser working** (`crawler_zyte.py::crawl_vessel_page`), validated on ship 869:
  emits a correct `Vessel` (current name PASIPHAE, IMO ID `imo-v-9289518`, flag/pastFlags/dates
  normalized from labels), the *current* owner + managers (Elyrion/Zeythra) as `imo-o-*`
  Companies with Ownership/UnknownLink relations, and a Sanction. Parses pages by
  label→value-sibling (`label_map`), owner/manager rows via the `Name (IMO / Country / Date)`
  regex. Country names ("Barbados", "Seychelles") normalize to codes automatically.
  Now also emits the P&I club (Organization + UnknownLink) and external reference links →
  sanction sourceUrl. `label_map` keys by `text_content` and scopes additional-info labels to
  "next sibling is .yellow" so value divs aren't mistaken for labels. A leftover-label audit
  (`VESSEL_SKIP_LABELS`) warns on any unmapped label → fail-loud on layout drift. Validated
  clean on ship 869.
- **No rekey** (see migration section): parser has no dependence on the source numeric id.
- **Conforms to xpath best-practices doc**: typed `h.xpath_elements`/`h.xpath_strings`/
  `h.element_text` (no raw `.xpath()`); `mypy --strict` passes.
- **Vessel enumeration wired** (`crawl_listing` → `crawl_vessels` → `crawl`): paginates
  `?page=N&per-page=12`, bounded by `listing_max_page` (out-of-range pages return content,
  not empty — empty-detection would loop forever). Validated on 8 real ships; counts sane
  (8 Vessel/Sanction/Ownership, 23 Company, 19 UnknownLink, 4 P&I Org). `unknown` flag
  values mapped to null via `type.country` lookup.
- **Ships-company pass done** (`crawl_company_page`, keyed `ua-ws-entity-<url_id>` per
  decision — matches the API crawler's id, zero migration). No public listing exists, so
  companies are **descended inline** as vessels link to them (`OPERATOR_LABEL` cell → company
  href); re-emitting across vessels is harmless. Emits LegalEntity (name/alias/
  registrationNumber/taxNumber/country/address) + Sanction(reason) + vessel→operator
  UnknownLink. Own `company_label_map` (col-sm-8 value, prev-sibling label) + leftover audit.
  Existing `type.identifier` lookups still apply (e.g. a mislabeled "TIN" → registrationNumber),
  so curated corrections carry over. Validated on companies 11664 and 14433.
- **mare.shadow done** (`crawl_shadow_fleet`): the shadow-fleet listing (~72 pages) carries
  each vessel's IMO in its card, so membership is read from the listing alone (no 864 detail
  fetches). The pass emits **stub vessels** (`imo-v-<imo>` + imoNumber + `mare.shadow` topic)
  that merge by id with the full vessels from the ships pass — no shadow state threaded through
  the parser; `crawl_vessel_page` stays a plain `(context, url)`. Validated.
- **Captains: skipped** by decision (not of interest).
- **Maritime cluster complete**: vessels + owner/managers/P&I + ships-company/operator +
  mare.shadow.

## Full crawler complete (2026-06-07)

All sections implemented in `crawler_zyte.py`, validated per-section and via a bounded
end-to-end `crawl()` smoke test (no unmapped-label warnings, every schema emitted), `mypy
--strict` clean.

- **Two reusable parsers**, GLEIF-style, driven by config tables `ENTITY_SECTIONS` /
  `PERSON_SECTIONS`:
  - `crawl_entity_page` (col-sm-8 layout) → LegalEntity, keyed `ua-ws-entity-<url_id>`.
    Sections: kidnappers/companies, uav/companies, stolen/companies, components/companies,
    rostec, sanctions/companies (+ ships-company, descended inline from vessels).
  - `crawl_person_page` (col-md-4 *or* col-sm-4 layout) → Person, keyed `ua-ws-person-<url_id>`.
    Sections: kidnappers/persons, sport/persons, propaganda/persons, stolen/persons,
    executives, sanctions/persons.
- **Multilingual solved without 3× fetch**: the Name cell lists uk/ru/latin forms as
  `<br>`-separated lines (`value_lines` via `itertext`), so one /en fetch yields all variants.
- **Label-alias handling** for section variants: citizenship (`Citizenship`/`Jurisdiction`),
  DOB (`Date and place of birth`/`DOB`), position (incl. executives' MIC-specific labels),
  links (`Links`/`Archive links`). Leftover-label audit warns on anything new.
- **Empty-entity / empty-person guards**: pages that don't match the expected layout (dead
  ids, non-company pages) are skipped with a warning, not emitted hollow.
- **Name `lang`** is set via script detection (`name_lang`): Latin→eng, Ukrainian-only
  letters→ukr, else Cyrillic→rus — recovering the language tags the API gave for free via
  separate name_en/ru/uk fields (the public site merges them into one `<br>` cell).
- **deathDate**: a "DD.MM.YYYY - DD.MM.YYYY" range in the birth field is split into birth +
  death. (A standalone death field is not exposed publicly — see gaps below.)
- **tools factories** (`crawl_tools`): the public /tools section is equipment; its ~218
  factory legal entities live at `/tools/company/<id>`, descended from equipment pages (the
  only public path). Heaviest section (~1.5k equipment fetches for 218 companies).
- **rostec ownership structure**: the "Within the structure of Rostec" holding chain is
  parsed into `Ownership` edges (each company emits its immediate-parent edge), mirroring the
  API's rostec/structure.

## Remaining API-vs-public gaps (audited)

- **Genuinely API-only** (not on public pages): standalone `date_death` (only recoverable
  when encoded as a birth-death range); `photo`/`logo` image URLs (the API crawler ignored
  these too). Name language tags are now recovered heuristically (above).
- **Coverage flag — verify**: `sanctions/persons` is **4385** records via the API but the
  public listing is ~**2196** (183 pages × 12) — the public site appears to show ~half
  (likely excludes lifted/historical). `sanctions/companies` matches (API 6737 ≈ public
  6744). Confirm whether the missing partner-sanction persons are intentionally unpublished.

## Remaining (operational cutover — not code)

- **Full validation run**: a real `zavod crawl` is ~13k+ Zyte fetches (sanctions/companies
  alone is 562 pages); not run yet. Do deliberately given cost.
- **entry_point / parallel-emit**: dataset still points at the API `crawler.py`. Cutover =
  run both (parallel-emit + dedupe), then switch `entry_point` to `crawler_zyte.py`.
- **Assertion retuning**: `.yml` min/max counts were set for the API crawler; counts will
  shift (e.g. captains dropped; partner sanctions persons/companies enumerated from the
  public site). Retune against the first full run.

## ID migration: parallel-emit + dedupe (decided)

No programmatic `rekey`, no frozen `api_id → imo` map. Migration uses the standard
duplication-in-the-middle pattern:

1. For an interim set of runs, **both** the API crawler (old `ua-ws-*` IDs) and the website
   crawler (new `imo-*` IDs) emit into the dataset.
2. **Dedupe links** old and new by shared IMO / name (IMO makes this reliable, especially for
   managers/vessels). The `NK-` cluster spans both.
3. Once clusters are stable, **phase out the API path**; only the website entities remain.

This sidesteps the manager/P&I rekey gap entirely (no need to recover the API's internal
manager id from public data). `crawler_zyte.py` therefore carries no rekey logic and no
dependence on the source's numeric ids — it's genuinely public-only.

## Open questions / decisions pending

1. **Name languages:** keep all three (3× fetch for persons/orgs) or English + original only?
2. **Cadence:** what frequency is acceptable given the fetch volume?
3. **Manager standalone pages:** confirmed managers appear on ship pages (name+IMO+country+
   date); verify whether they also have standalone public pages or must be sourced inline.
4. **Where to start coding:** vessels section first (richest, proven-divergent, exercises the
   IMO-ID + manager-overlay + rekey paths end to end) as the reference implementation.
