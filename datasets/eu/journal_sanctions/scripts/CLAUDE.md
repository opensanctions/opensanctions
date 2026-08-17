# Consolidated parser scripts

One script per framework act (`parse_32014R0833.py` for Russia 833/2014,
`parse_32016R0044.py` for Libya 2016/44, …). Each parses the pinned
consolidated XHTML expression of its regulation into
`../data/consolidated/{framework}.csv` under the contract in
`../data/FORMAT.md`.

These scripts are maintained by an agentic loop: when a source document
evolves, the parser breaks with an error naming the annex and the problem,
and the agent fixes **that one file**. Everything below exists to protect
that property.

## Ground rules for code sharing

These are guidelines, not hard rules — but deviations should be argued in
review.

1. **One framework act = one self-contained parser.** Every document decision
   lives in the parser file: annex inventory and classification, part-heading
   rules, table headers, label vocabularies, accepted date formats, per-entry
   quirk pins, the CLI. Two documents that look similar today will diverge;
   the agent fixing one parser must never need to reason about another.
2. **Copy first, share opportunistically.** The default way to reuse a
   sibling parser's behavior is to copy the function and adapt it freely.
   A function moves into `common.py` only once it is provably
   framework-agnostic.
3. **`common.py` is a flat library of leaf utilities and passive data
   structures.** Many small functions beat one configurable object. A
   function belongs there only if it is (a) a pure function or constant over
   strings/elements/records, (b) identical for all parsers *by necessity* —
   mandated by the CSV contract, by the EUR-Lex consolidation markup standard,
   or by the registries that own the sanctions vocabulary, not by coincidence
   — and (c) called with plain data arguments. Shared dataclasses that
   *describe* things (`AnnexSpec`, `Row`) are welcome: joint vocabulary is
   cheap as long as it carries data, not behavior. Each parser decides for
   itself how to interpret them.
4. **No inversion of control.** Shared code never calls back into parser
   code: no registries, dispatch tables, role→handler maps, or CLI builders.
   Parsers call `common.py` top-down, like a stdlib. If sharing something
   seems to need a callback or a config object, copy it instead.
5. **Widening a shared function for one document is allowed — at fleet cost.**
   A change inside one parser is validated by that parser's snapshot diff; a
   change to `common.py` must be re-validated against every parser that
   exists, which makes it roughly N× as expensive as the local edit. Most
   single-document quirks are cheaper to absorb by copying the function into
   the parser and adapting the copy. Widen `common.py` only when the widening
   is genuinely framework-agnostic and worth the fleet-wide re-run.
6. **Fail-closed policy stays local.** Each parser must reject structures its
   document has not shown — so *what is legal* (accepted date formats, known
   labels, expected parts and annexes) is parser code, even when *how to read
   it* (date primitives, line access) is shared. Never widen a rule beyond
   the formats actually observed in the document; a new format is a code
   review event, not a fallback case.
7. **Blast-radius check.** After any change to `common.py`, re-run every
   parser in the Makefile against its pinned consolidated version (`make
   parse`) and require a clean `git diff` on every snapshot CSV — unless
   changed output is the point of the change, reviewed per file.
8. **Duplication between parsers is an accepted cost, not a smell.** Do not
   deduplicate on sight. Distill into `common.py` only after copies have
   proven byte-stable across parsers, and even then apply rules 3–5.

## The two contract tools

`validate.py` and `format.py` are not parsers and the rules above do not
govern them. They implement `../data/FORMAT.md` itself — `validate.py` checks
any reviewed CSV against it, `format.py` puts a transcribed amendment file's
header into the contract's column set and order.

For these two, rule 2 is inverted: they **must** read the column contract from
`common.py` (`METADATA_COLUMNS`, `ENTITY_COLUMNS`, `CONSOLIDATED_COLUMNS`,
`AMENDMENT_COLUMNS`) and the cell codec from `join_multi` / `split_multi`,
never restate them. A second copy of the column list is how the contract drifts
from the files that are supposed to satisfy it.

Note that `split_values` is not the inverse of `join_multi`: it splits printed
source wording, which carries no CSV quoting, and parsers use it on document
text. `split_multi` decodes a contract cell. Do not merge them.

## Mechanics

- Run from the dataset directory:
  `python scripts/parse_<celex>.py <consolidated-celex>` — the consolidated
  version must match the `config.consolidation` pin in the dataset YAML and
  the Makefile, updated in the same commit as the CSV.
- `out/` caches fetched source expressions and is gitignored; `--source`
  parses exact local bytes instead.
- No `__init__.py` in this directory; scripts import `common` as a sibling
  module.
- After parsing, validate: `python scripts/validate.py` (or `make all`).
