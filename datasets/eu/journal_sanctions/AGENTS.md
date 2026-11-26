# Transcribing EU sanctions acts

This dataset turns the EU legal acts that list, amend and delist sanctions
designations into op-log CSVs under `data/regulations/`. The contract for
those files is [data/FORMAT.md](data/FORMAT.md).

Acts vary across regimes and years: annex tables, labelled prose blocks,
whole-annex replacements, omnibus acts that touch many frameworks at once.
So transcription is an agentic process with a harness on either end:

1. **Input harness** — prepares an act for reading: which acts exist,
   fetching them, a lossless plain-text rendering, and the facts that need no
   judgement (publication and entry-into-force dates).
2. **Extraction** — the agent reads the act, decides what it does to the
   framework's list, and writes the rows. This is the only stage that
   interprets.
3. **Output harness** — enforces the contract and does the mechanical work:
   minting and matching ids, replay consistency, checks against the act and
   against EUR-Lex consolidations.

The harnesses own whatever can be checked mechanically; the agent owns every
interpretation.

## Transcribing an act `C` of framework `F`

The acts of `F` — including the English corrigenda of `F` and of its acts —
come from `tools.common.related_acts(F)`; transcribe them in date order. Run
the tools from this directory as `python -m tools <command>`.

1. `python -m tools act C` fetches the act into `tools/out/C/` — from the
   local corpus (see below) when it holds the act, else from CELLAR: `body.html`,
   `meta.json` (publication and entry-into-force dates) and `text.txt` (one
   line per paragraph or table row, nothing dropped).
2. Read the act and decide what it does to `F`'s list.
3. Write `data/regulations/F/C.csv` with `id` left empty, by hand or with a
   throwaway script in `tools/out/C/` run by `python -m tools run SCRIPT`,
   which builds rows with `tools.common.encode_row` and writes them with
   `tools.common.write_oplog`. `programKey` and `measure` come from the
   regime's program in `meta/programs/`. A `MOD` that changes part of an
   entry starts from the target's current row:
   `python -m tools entry F ID --as-mod C --out tools/out/C/row.csv` (or
   `--annex A --record N` for `ID`; `--name` ranks candidates).
4. `python -m tools resolve F data/regulations/F/C.csv` mints ids for `ADD`
   rows, matches `MOD`/`DEL` rows to the entries they amend, checks ids filled
   in by hand, and reports likely listings of a new designee in other
   annexes. For an act that replaces a whole annex,
   `--delete-untouched --annex X` adds `DEL` rows for the entries it leaves
   out. Pin an unresolved row with `--pin LINE=ID` only when the act's text
   settles it.
5. `python -m tools check F --online` checks the stack against the contract
   and the replay, every fetched act against its file, and the files against
   CELLAR's act list (up to the stack's newest file). Fix every error and
   account for every warning.
6. At each EUR-Lex consolidation dated on or after `C`
   (`query_act(F).consolidated` on a `zavod.shed.ojeu.cellar.cli_client()`,
   a context manager), run `python -m tools compare F <consolidated CELEX>`.
   It checks the live entries' values against the consolidation's text and
   the acts and corrigenda its header cites; count the entries it prints per
   annex part against the live counts it reports, since it cannot see
   entries the replay lacks.

When the contract's columns change, `python -m tools migrate` brings the
files up to date.

## Review

A transcription goes to review with the CSV diff and the `check` and
`compare` reports. State every pinned match, every accepted warning, and
every judgement the act's text did not settle on its own.

## Shared extraction helpers

Helpers for recurring extraction problems are plain functions called by an
extraction script, admitted only with evidence that the problem recurs
across regimes. `python -m tools corpus` downloads every act of the covered
frameworks to `data/tmp/eu_oj/` at the repository root, to search with `rg`.
