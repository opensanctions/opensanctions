---
name: review-crawler
description: Review a new or changed crawler (its dataset .yml and code), given a dataset .yml path or a PR, against the merge checklist in zavod/docs, and draft inline PR comments on request. Use when asked to review a crawler, a dataset or a crawler PR.
argument-hint: "<PR URL or number | dataset .yml path>"
allowed-tools: Read, Write, Glob, Grep, Bash, WebFetch, WebSearch, Agent
---

# Review a crawler

Review the crawler at: $ARGUMENTS

## 1. Get the code

If given a PR (URL or number), check out its head in a worktree in the scratchpad, so the
user's checkout stays untouched:

```bash
gh pr view <n> --json title,author,body,files
gh pr diff <n>
git fetch origin pull/<n>/head:review-<n>
git worktree add <scratchpad>/review-<n> review-<n>
```

If given a dataset path, diff its directory against `origin/main`, including uncommitted
changes.

## 2. Run the merge checklist

See [the merge checklist](../../../zavod/docs/best_practices/merge_checklist.md). Go
through it item by item and record whether the crawler meets each one, misses it, or it
doesn't apply, with the evidence. Double-check each miss before reporting it, and drop
what you can't support.

## 3. Report

In chat: the checklist with one row per item, then the misses, numbered, each with
`file:line`, the evidence and the checklist item it fails. List anything that looks wrong
but the checklist doesn't cover as a question for the reviewer, not as a change request.

If given a dataset path, stop here and offer to fix the misses.

## 4. Draft inline comments

If given a PR, and only when the user asks, draft comments for the misses they pick.
Write the review to `<scratchpad>/review_<n>.json`:

```json
{
  "event": "COMMENT",
  "body": "",
  "comments": [{"path": "datasets/xx/foo/crawler.py", "line": 42, "body": "..."}]
}
```

`line` is the line number in the PR head's version of the file. Show the drafts in chat
and **post only after the user explicitly approves**. Never reply to existing comments.

```bash
SHA=$(gh pr view <n> --json headRefOid --jq .headRefOid)
jq --arg sha "$SHA" '. + {commit_id: $sha} | .comments |= map(. + {side: "RIGHT"})' \
  <scratchpad>/review_<n>.json \
  | gh api -X POST repos/opensanctions/opensanctions/pulls/<n>/reviews --input - \
      --jq '{state, html_url}'
```

Remove the worktree when done: `git worktree remove <scratchpad>/review-<n>`.
