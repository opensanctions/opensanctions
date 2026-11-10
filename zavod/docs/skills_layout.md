# Skills layout

AI agents working in this repository take their instructions from several places. Each
rule has one home, and every other place points to it.

## Layers

- `zavod/docs` holds every rule and method, one home each, including worked examples.
- `.claude/skills` holds agent procedure only.
- `.claude/docs` is legacy and is being moved into `zavod/docs`. Add nothing new to it.
- The root `CLAUDE.md` holds routing and the core coding conventions.

## Rule or procedure

A statement that decides what a crawler emits or how its code is written is a rule. It
lives in `zavod/docs`, on a new page if none fits.

A statement about what the agent does is procedure, and lives in the skill: checkpoints,
subagents, scripts to run, what to check and report, when to ask the user. A review skill
runs [the merge checklist](best_practices/merge_checklist.md); a missing check goes into
the checklist.

## Pointers

A skill refers to a rule with a bare `See [page](…)`. A link with words about the rule
beside it is a copy. Skills cite only `zavod/docs`, never other skills or external
documents.
