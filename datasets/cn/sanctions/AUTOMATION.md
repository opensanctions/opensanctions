---
description: Notes toward automatically discovering new official Chinese sanctions notices for human review.
date: 2026-06-29
tags: [cn_sanctions, monitoring, source-discovery, human-review]
---

# Monitoring Chinese sanctions sources

## Goal

Detect new official notices that may contain sanctions designations and present them
for human review. The monitor should reduce the chance that a notice is missed; it
should not add or remove rows in `sanctions.csv` automatically.

The CSV remains the reviewed source of truth. Every designation added to it must cite
the official notice from which it was extracted.

## Initial sources

| Authority | Index | Useful title terms |
| --- | --- | --- |
| Ministry of Foreign Affairs | [Anti-sanctions notices](https://www.mfa.gov.cn/web/wjb_673085/zfxxgk_674865/gknrlb/fzcqdcs/) | `采取反制措施` (take countermeasures) |
| Ministry of Commerce | [Security and Control Bureau policy notices](https://aqygzj.mofcom.gov.cn/flzc/gzjgfxwj/index.html) | `采取反制措施`, `列入不可靠实体清单`, `列入出口管制管控名单`, `列入关注名单` |
| Taiwan Affairs Office | [Measures against Taiwan independence](https://www.gwytb.gov.cn/zccs/zccs_61195/cjtdwgfz/bt/) | `“台独”顽固分子`, `“台独”打手帮凶` |

Keyword matches are discovery hints, not proof that a page contains a designation.
The Taiwan Affairs Office does not use a consistent notice-title format. MFA measures
announced only by an embassy will not necessarily appear in the central index and
need a separate discovery strategy.

MOFCOM publishes a stream of announcements rather than a structured snapshot of the
current lists. Monitoring must therefore detect notices that add, remove, suspend, or
amend measures and reconstruct cumulative state from those events. The Export Control
List is based on Article 28 of the Regulations on Export Control of Dual-use Items
(State Council Regulation No. 792), effective from 1 December 2024.

## Proposed flow

1. Fetch each index and follow pagination far enough to overlap the previous run.
2. Extract the notice URL, Chinese title, publication date, and issuing authority.
3. Canonicalize URLs and compare them with known notices.
4. Rank unseen notices using authority-specific keywords and page text, including
   language indicating removal, suspension, amendment, or cancellation.
5. Emit a review report containing the notice metadata and the reason it matched.
6. A reviewer reads the official notice and submits the resulting CSV rows in a PR.

The first version should stop at notice discovery. Parsing names and measures from the
notice can be added later, once monitoring has shown that source discovery is stable.

## Tracking reviewed notices

`Source URL` values in `sanctions.csv` identify notices that produced designations,
but they are not enough to represent monitoring state. A reviewed notice may be a
false positive, contain only a policy change, or duplicate a notice published at a
different official URL.

The monitor will therefore need a small reviewed-notice manifest with, at minimum:

- Canonical official URL.
- Authority, title, and publication date.
- Review status: accepted, no designations, duplicate, or needs follow-up.
- Optional replacement URL and notes explaining the decision.

This state should be reviewable in Git. It must not use CSV row numbers or entity names
as identifiers because both can change independently of the source notice.

## Review output

A candidate report should include:

- Official URL and authority.
- Original Chinese title and publication date.
- Matched keyword or other discovery rule.
- Whether another known notice has the same title or date.
- Best-effort archived URL, if one was created.

Initially, produce one periodic report or GitHub issue rather than one issue per
notice. Do not automatically open a data PR: deciding who is designated, which
program applies, and when a measure starts often requires interpretation.

## Link rot and removals

Preserve the original official URL even when it stops resolving. A deleted page is not
evidence of delisting. Archiving should be best-effort and should not block ingestion;
an archive URL supplements rather than replaces the official citation.

Likewise, the monitor must not infer an end date from a notice disappearing. A measure
ends only when an official notice explicitly suspends, cancels, or establishes the
expiry of that measure.

## Reliability checks

The monitor should fail visibly when an index cannot be parsed or changes structure.
A successful HTTP response with zero extracted links is an error, not a successful
empty run. Useful checks include:

- A minimum number of links extracted from each index.
- Stable parsing tests based on saved index and notice fixtures.
- Deduplication tests for relative, redirected, mobile, and translated URLs.
- A test that previously reviewed false positives are not reported again.
- A bounded lookback so pagination or ordering changes do not hide late additions.

## Possible later stages

1. **Discovery:** report unseen, likely relevant official notices.
2. **Deterministic extraction:** parse structured attachments and consistently formatted
   name lists into draft records.
3. **Assisted extraction:** use an LLM to propose CSV rows with source text excerpts,
   while requiring human verification of every field.

No stage should publish designations without review. Automated extraction must retain
the exact official notice URL and enough source context for a reviewer to reproduce
the result.

## Open questions

- Where should reviewed-notice state live, and who marks candidates as reviewed?
- Should the monitor create a GitHub issue, a build artifact, or both?
- How should embassy-only MFA announcements be discovered?
- Should official pages be saved as dataset resources in addition to Wayback capture?
- How should suspended measures be represented when they are not fully terminated?
- Which Taiwan Affairs Office categories should become formal sanctions programs?
