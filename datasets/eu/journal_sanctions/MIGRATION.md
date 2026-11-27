# Migrating a framework to the op-log

The crawler publishes most frameworks from `data/consolidated/` (one parsed
EUR-Lex consolidation per framework, pinned in `eu_journal_sanctions.yml`)
and `data/amendments/`. This file is the process for moving one framework
from that data to the op-log under `data/regulations/`. It is deleted, with
`tools/legacy.py`, `scripts/` and the old data, when the last framework has
moved.

The per-act loop is [AGENTS.md](AGENTS.md); the file contract is
[data/FORMAT.md](data/FORMAT.md). Read both first.

## Status and claiming

`python -m tools legacy-status` lists every pinned framework: its program,
pin, entries in the old snapshot, acts filed against the acts CELLAR relates
(`acts`), the newest filed act, and the state: `not started`, `in progress`,
`transcribed`; for a transcribed one, whether its IDs are backported.
`--markdown` prints the table kept in the PR description.

Claim a framework on the PR or in the team channel before starting, and work
on a branch of your own. Frameworks with small or empty snapshots are the
cheapest; Libya (32016R0044) is partly done and stops at 2021/2192. Big ones
(881/2002 with about 380 acts, 833/2014, 36/2012) take several rounds.

## Setup

- Run everything from this directory with the Python environment that has
  `zavod` installed: `python -m tools <command>`.
- `python -m tools corpus` downloads every act of the covered frameworks to
  `data/tmp/eu_oj/` at the repository root. `act` and `check --fetch` read
  from it before falling back to CELLAR, and `rg` over it finds how other
  acts printed the same thing.
- `tools/out/` is scratch space and is not committed. Extraction scripts
  written there are throwaway and specific to one act or regime; write new
  ones rather than adopting another regime's. The `FRICTION*.md` logs and
  scripts from earlier rounds predate the current FORMAT.md, and where they
  disagree with it, FORMAT.md is right.

## Steps for framework `F`

1. **Transcribe to the present**, act by act, per AGENTS.md. This is usually
   delegated to an agent; see below. `related_acts(F)` gives the acts.
2. **Verify.** `python -m tools check F --online --fetch` has no errors, and
   every warning is accounted for. Run `python -m tools compare F <CELEX>` at
   the pinned consolidation and at the latest one; read its errors as below.
3. **Compare with the old snapshot.** `python -m tools legacy-compare F`
   replays the stack as of the pin and pairs it with
   `data/consolidated/F.csv`. A clean result has:
   - the same entry count in every annex part, and no `UNMATCHED` rows;
   - column differences that FORMAT.md explains: schemas from printed legal
     forms, identifiers in their own columns, values split where the source
     separates them, metadata dropped;
   - few entries with words only in the old snapshot (`--details` lists
     them). The expected ones are print differences (see below); anything
     else is a value the transcription lost.
4. **Fix defects.** The `check` warnings name values in the wrong column. A
   fix that touches a value carried through many `MOD`s is a one-off script
   in `tools/out/migrate/` that transforms each value the same way in every
   file of the stack, run once without and once with `--apply`, followed by
   `check --fetch` to prove every moved value is still printed in its act.
5. **Backport IDs.** `python -m tools legacy-ids F --out tools/out/ids/F.csv`
   pairs every designation with the entity ID the crawler publishes; it must
   report no errors. Then `python -m tools rewrite-ids F tools/out/ids/F.csv`.
   Running `legacy-ids` again reports nothing to rewrite.
6. **Review.** State what AGENTS.md's review section asks for, add the
   `legacy-compare` summary, and update the PR table.
7. **Switch.** The crawler cannot publish a framework from the op-log yet.
   Once it can, a framework switches when steps 1–5 are done; its
   `data/consolidated/` file, `scripts/parse_F.py` and pin then go.

## Delegating to an agent

An agent in its own git worktree can transcribe 100+ acts in one to three
hours. Worktrees branch from `main`, so the prompt has to set them up. A
prompt that worked:

> Transcribe the remaining acts of framework `F` into the op-log, up to
> today. Setup: `git reset --hard <your branch>`; `cd
> datasets/eu/journal_sanctions`; link `data/tmp/eu_oj` of the main checkout
> into the worktree; copy (not link) the main checkout's `tools/out`. Read
> AGENTS.md and data/FORMAT.md in full first. Scope: the acts of
> `related_acts("F")` without a file, in date order, starting at `<CELEX>`.
> Follow AGENTS.md steps 1–6 for each act. Commit each act locally as
> `[eu_journal_sanctions] Transcribe <celex> into the <regime> op-log`; never
> push; never commit `tools/out/`. `check` stays at 0 errors at every
> commit. Don't invent rules: where the text doesn't settle something, choose
> by FORMAT.md's principles and record it. Fix any toolkit bug with a test, in
> a separate commit. Don't change FORMAT.md or AGENTS.md; propose. Stop
> cleanly at an act boundary if you run low. Before reporting, account for
> every `check` warning on your rows. Report: the last act and commit range;
> `check` and `compare` results with live counts per annex part; every pinned
> match, accepted warning, `extra` value and judgement the text didn't settle;
> toolkit changes; the worktree path and branch.

To merge, fast-forward or cherry-pick its commits, then re-run the tool
tests, `check` and `compare` in your own checkout: the report is the agent's
claims, not evidence. Read toolkit changes and commits that rewrite earlier
rows closely. Remove the worktree when done.

## Reading `compare` errors

`compare` checks the replay against a consolidation's text, so some errors
are differences in print, not transcription mistakes:

- the consolidation fixes or introduces typos: doubled full stops, hyphen
  spacing, dropped apostrophes;
- the acts print Latin "i" inside Cyrillic names, or other Persian letter
  forms;
- names the acts print only as images (Korean, Chinese, some Persian) are
  text in the consolidation;
- EUR-Lex redacts some entries as "(*n)".

The files keep the act's text. An act the consolidation cites but CELLAR
does not relate to the framework (2023/426, recorded as amending a Decision)
is filed by hand; `check` warns about it. Corrigenda published after a
consolidation's date are folded into `compare`'s view.

## The old snapshot's quirks

`legacy-compare` and `legacy-ids` absorb these, but they explain the deltas:
aliases kept inside the name ("O Rang (Po Thong Gang)"), "Text of image" and
script labels as aliases, lettered entries with an empty number, numbers
that differ between the acts and the consolidation, and document metadata
inside identifiers.

## Extraction defects seen so far

`check` warns about each of these: "; " lists stored as one value, combined
INN/KPP/OGRN lines, dates in `phone`, URLs in `email`, "Co., Ltd." split
into two aliases, and labelled values ("Father's name:", "Physical
description:", "Associated entities:") left in `notes`.
