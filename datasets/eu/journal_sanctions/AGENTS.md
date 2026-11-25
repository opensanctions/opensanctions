# Transcribing EU sanctions acts

This dataset turns the EU legal acts that list, amend and delist sanctions
designations into op-log CSVs under `data/regulations/`. The contract for
those files — columns, operations, replay, fidelity — is
[data/FORMAT.md](data/FORMAT.md).

Acts vary across regimes and years: annex tables, labelled prose blocks,
whole-annex replacements, omnibus acts that touch many frameworks at once.
So transcription is an agentic process with a harness on either end:

1. **Input harness** — prepares an act for reading: which acts exist,
   fetching them, a lossless plain-text rendering, and the facts that need no
   judgement (publication and entry-into-force dates, what the act amends).
2. **Extraction** — the agent reads the act, decides what it does to the
   framework's list, and writes the rows. This is the only stage that
   interprets.
3. **Output harness** — enforces the contract and does the mechanical work:
   minting and matching ids, replay consistency, plausibility warnings,
   comparison with EUR-Lex consolidations.

The harnesses own whatever can be checked mechanically; the agent owns every
interpretation.

## Transcribing an act `C` of framework `F`

Run the tools from this directory as modules. `tools/out/` is a gitignored
scratch area with one directory per act.

1. `python -m tools.act C` fetches the act into `tools/out/C/`: `body.html`,
   `meta.json` (publication and entry-into-force dates) and `text.txt` (one
   line per paragraph or table row, nothing dropped).
2. Read the act and decide what it does to `F`'s list.
3. Write `data/regulations/F/C.csv` with `id` left empty, by hand or with a
   throwaway script that stays in `tools/out/C/`. A script builds each row
   with `tools.common.encode_row`, which encodes multi-valued cells, and
   writes them with `tools.common.write_oplog`. `programKey` and `measure`
   come from the regime's program in `meta/programs/`; the regime's
   `data/consolidated/{F}.csv` shows the program, measure and annex choices
   made so far.
4. `python -m tools.lookup resolve F data/regulations/F/C.csv` mints ids for
   `ADD` rows and matches `MOD`/`DEL` rows to the live entries they amend.
   It lists the live entries the act does not touch; for an act that
   replaces a whole annex, `--delete-untouched` writes their `DEL` rows. An
   unresolved row prints its candidates: check it with
   `python -m tools.lookup find`, and pass `--pin LINE=ID` only when the act's
   text settles it.
5. `python -m tools.validate F --online`, and account for every warning.
6. `python -m tools.verify F C` checks that every value is printed in the
   act and that the dates match CELLAR's.
7. At each EUR-Lex consolidation dated on or after `C`
   (`query_act(F).consolidated`), run
   `python -m tools.replay F --compare <consolidated CELEX>`, which checks
   every live entry's values against the consolidation's text. It cannot see
   entries the consolidation prints but the replay lacks: check those by
   reading.

## Review

A transcription goes to review with the CSV diff and the validate, verify and
compare reports. State every pinned match, every accepted warning,
and every judgement the act's text did not settle on its own.

## Shared extraction helpers

A small library of helpers for recurring extraction problems may follow. A
helper is a plain function over strings or elements, called by an extraction
script. It is admitted only with evidence from the corpus that the problem recurs
across regimes: `python -m tools.corpus` downloads every act of the covered
frameworks to `data/tmp/eu_oj/` at the repository root, to search with `rg`.
