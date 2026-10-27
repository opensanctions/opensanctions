---
name: review-docs
description: Review a docs change for duplicated rules and overcompensating fixes. Use when asked to review or tighten a docs PR, or before committing docs or skill edits.
argument-hint: "[PR number | branch]"
allowed-tools: Read, Glob, Grep, Bash
---

# Review a documentation change

Target: $ARGUMENTS (default: the current branch against `main`).

Docs changes usually settle a question: a reader, often an LLM writing a crawler, got
something wrong, or reviewers kept answering the same question. Two principles keep the
answer from making the docs worse:

- **Each rule or definition has one home.** See `zavod/docs/skills_layout.md`. When two
  places state the same rule, keep the better one as it is and delete the other; don't
  merge the two into new text.
- **Fix the text that misled; don't argue with the misreading.** A misreading usually
  comes from an undertone in the existing text, an example, or a skill requirement.
  Correct or remove that. A sentence added to permit or forbid the thing that went wrong
  ("X is fine") is a new undertone and causes the next misreading.

## Steps

1. **List the questions the change settles**, from the PR body and commits, or ask.
2. **For each question, find every place the docs speak to it.** Grep `zavod/docs`,
   `.claude/skills`, and `.claude/docs`, including examples and skill requirements, not
   just the diff. Then:
   - **More than one place:** pick the home per `zavod/docs/skills_layout.md`, and delete
     the rest (see above).
   - **One place, which misled:** quote the sentence, example or requirement that misled;
     the fix edits that, and only that. If you can't quote one, treat the question as a
     gap. Flag added sentences that argue against the old reading, rewrites beyond the
     misleading text, removed content that wasn't misleading, wording made stronger than
     the decision ("consider" → "must"), and examples changed to model an optional step.
   - **No place:** the fix is one or two plain sentences in the topic's home: the rule,
     with at most a clause of reason. No failure narrative, no "must never".
3. **Check the change adds no new copies.** Every rule the diff adds or rewords appears
   in one place only, across docs and skills. Check added or changed skills against
   `zavod/docs/skills_layout.md`, and that every pointer resolves.
4. **Check the diff's size against the questions.** Every hunk belongs to a listed
   question, and each question accounts for a few lines. A moved or rewritten section, or
   a hunk no question explains, gets reverted.
5. **Read each touched page as a new reader.** It should read as if it had been right all
   along. If you can reconstruct the dispute from the page, the change overcompensates.
6. **Apply the [FtM documentation style guide](https://github.com/opensanctions/followthemoney/blob/main/docs/styleguide.md)**
   to changed lines only.
7. **Report** per question: its home, the copies found, the misleading text if any, and
   the minimal edit. Don't edit files unless asked.

## Example

A PEP crawler skipped official emails. The cause was the reference crawler's comment
`# Contact details are not extracted for PEPs.`, with `email` in its drop list, while
`peps/index.md` already prohibited only phone numbers and private addresses. The fix corrects
the comment and emits the email. Adding "Official email addresses are fine." to
`peps/index.md` argues with the misreading and isn't needed.
