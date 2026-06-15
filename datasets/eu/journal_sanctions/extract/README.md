# OJ sanctions annex extractor — brief for future instances

`extract.py` turns one EU Official Journal sanctions notice into rows for the
`eu_journal_sanctions` source spreadsheet. It is a **data-entry aid, not a
crawler**: a human reviews its CSV and pastes it into the "Unconsolidated" and
"Context" tabs of the sheet. It never touches the sheet or the pipeline.

```
python datasets/eu/journal_sanctions/extract/extract.py 32026D1364
# -> extract/out/32026D1364.csv  (gitignored)
```

**Expect to change the code for almost every new notice.** Each amending act and
each sanctions regime uses slightly different identifying-info labels, table
layouts, and conventions. The tool is a best-effort first pass that is designed
to *show you what it couldn't handle* rather than guess. Extending the small
label sets at the top of the file is the normal workflow, not a failure.

## Where the data comes from (and the WAF)

- The EUR-Lex front end (`eur-lex.europa.eu`) sits behind an AWS WAF that returns
  **HTTP 202 + a JavaScript bot-challenge** (a `gokuProps` blob, ~2 KB, no
  content) to scripted requests. Do **not** try to solve/bypass it.
- Fetch from the Publications Office **CELLAR** repository instead, which is not
  WAF-protected: `http://publications.europa.eu/resource/celex/{CELEX}` with
  `Accept: application/xhtml+xml` and `Accept-Language: eng`. Only the **XHTML**
  manifestation exists — `text/html` returns 400/404, and the language header is
  required.
- Structure: annex entries live in `table.oj-table` elements with columns
  `[«entry no.», Name, Identifying information, Reasons, Date of listing]`. Parse
  with `h.parse_html_table(header_tag="td", index_empty_headers=True)`; the empty
  first header becomes `column_0`.

## Validation & cross-verification — read this part

The source is government data entered by hand: inconsistent labels, wrapped
cells, stray punctuation, mixed date formats. The whole value of this tool is in
**catching where it guessed wrong**, so treat every run as something to verify,
not trust.

1. **The run summary is the primary signal. Always read it.**
   - *Unmapped labels* — every new notice tends to introduce label variants
     (`TIN`, `SNILS`, `Russian website`, `Position` vs `Function`, `Statement of
     Reasons` vs `Reasons`). Unmapped values are preserved verbatim in a `[...]`
     block in **Notes** so nothing is lost, but you must decide: map it to a
     column, or accept it staying in Notes (e.g. `phone`, `stock code` have no
     column). Add new variants to the label sets at the top of the file.
   - *Persons / Entities counts* — sanity-check against the actual document.

2. **Re-run every prior CELEX and diff after any parser change.** This is the
   single best regression check. Past diffs revealed real bugs *and*
   improvements. Known-good documents to keep re-running: `32026D1364` (9 P / 45
   E, the richest — companies, IMO numbers, dual nationality, multi-value IDs),
   `32026D1351` (entity with no heading), `32026D1363` (TIN/SNILS/dual website).

3. **Check column fill rates, not just row counts.** A column that is empty
   across many rows, or one polluted with text from a neighbour, means a parsing
   bug. Real examples found this way:
   - "Date of registration" continuation leaked a credit-code line into **DOB**.
   - `Tel.`/`Fax` lines (no colon) were appended to **Address**.
   - Multi-line `Function` ("Businessman / Head of JSC …") was truncated.
   The fixes: colon-less lines continue the previous value **only** for wrapping
   fields (Function/Address/POB); telecom lines are always leftovers.

4. **Verify person-vs-entity classification per document.** Some notices have no
   "Persons"/"Entities" headings and table persons and entities separately. The
   fallback classifies each row by whether it carries personal-detail labels
   (DOB/nationality/gender/function). An entity with *only* an address once
   slipped through as a Person — eyeball any small/second table and the `Type`
   column.

5. **`List ID` is the EU annex entry number, and it is per-regime.** Each amended
   Decision keeps its own person/entity numbering: `2014/145` (Russia/Ukraine)
   persons were at ~2047, entities ~774; `2023/891` (Moldova) and `2024/2643`
   (Russia destabilisation) start their own sequences. **Do not** assume numbers
   are unique or continuous across documents — confirm which act the notice
   amends (it's in the preamble) before pasting, and match the right tab/regime.

6. **Confirm Country is a country, not a city.** Nationality demonyms are mapped
   to territory names (`Russian` → Russia); place-of-business / place-of-
   registration are reduced to their trailing component (`Moscow, Russian
   Federation` → Russian Federation). New demonyms pass through unchanged — watch
   for ones not yet in the map.

7. **Match the sheet's existing conventions, not your intuition.** Open the live
   sheet and copy how filled rows look before inventing a mapping. Established
   ones: **Notes = the listing reasons text**; entity **DOB = incorporation
   date**; identifiers split across `taxNumber` (INN/TIN), `registrationNumber`
   (OGRN), `kppCode`, `imoNumber`, `idNumber` (OKPO/SNILS).

8. **Spot-read the rendered annex for tricky rows.** For dual nationality,
   multi-value passports, or aliases in other scripts / parentheses, compare a
   few output rows against the actual document text. Don't reformat or "clean"
   source names beyond stripping label prefixes — the matcher normalizes case.

## Design principle

Fail loud and visible. Anything ambiguous or unrecognised is surfaced (summary
line + Notes leftover), never silently dropped, because a human is the last line
of defence before this data reaches the sheet. Keep it that way when you extend
it.

The label maps and helpers in `extract.py` are the authoritative spec for the
current field handling; this README is the *why* and the review discipline.
Sheet: `1rauQMdCYTjTwmSzqfUvur1SfkCGYwfRn6_e5_oX39EY`.
