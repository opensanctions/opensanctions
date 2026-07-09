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

## Implemented sources

The crawler's `discover_candidates()` polls four indexes. MFA and TAO are matched by
notice-URL pattern (every notice on the index is a candidate; non-designations are
muted through `reviewed_urls`). Both MOFCOM indexes filter on title keywords because
their columns mix designation notices with unrelated regulatory documents.

| Authority | Index | Match strategy |
| --- | --- | --- |
| Ministry of Foreign Affairs | [Anti-sanctions notices](https://www.mfa.gov.cn/web/wjb_673085/zfxxgk_674865/gknrlb/fzcqdcs/) | notice-URL pattern |
| Ministry of Commerce (annual) | `zcfb/blgg/gg/{year}` announcement indexes | title keywords |
| Ministry of Commerce (industry security) | [Security and Control Bureau notices](https://aqygzj.mofcom.gov.cn/flzc/gzjgfxwj/index.html) | title keywords |
| Taiwan Affairs Office | [Measures against Taiwan independence hub](https://www.gwytb.gov.cn/zccs/zccs_61195/cjtdwgfz/) | notice-URL pattern |

Useful title terms for the keyword-filtered MOFCOM sources: `采取反制` (take
countermeasures; the loose stem also matches the early `采取反制裁` variant),
`列入不可靠实体清单`, `列入出口管制管控名单`, `列入关注名单`, and the state-change stems
`移出`/`暂停`/`取消` combined with a program name or `反制措施`. Keyword matches are
discovery hints, not proof that a page contains a designation.

The TAO hub is crawled at its parent path (not the `/bt/` subpath) so new designation
categories appearing as new subpaths are still caught; its notice titles are
inconsistent, so it is matched by URL rather than keyword. MFA measures announced only
by an embassy will not necessarily appear in the central index and remain a separate
discovery gap tracked by the dataset's `manual_check` reminder.

### Source characteristics to preserve

- The TAO hub is served as GB2312/GB18030, not UTF-8; the fetch transcodes explicitly.
- The aqygzj index is a JavaScript shell whose list is served by a JPaaS CMS API. The
  front-end requests the entire list in one call and paginates client-side, so passing
  `paramJson={"pageNo":1,"pageSize":99999}` returns every notice in a single request —
  there is no server-side paging to iterate. The `webId`/`pageId`/`tplSetId` query
  values are CMS-internal and will change on a site redesign, so the parser fails loudly
  when the JSON response shape changes rather than reporting an empty run.
- The aqygzj notices use `/flzc/gzjgfxwj/art/...` URLs, distinct from the `blgg/gg`
  copies. A notice already represented under a `blgg` URL therefore still surfaces here
  under its aqygzj URL until that URL is added to `reviewed_urls`. Its unique value is
  the `商务部令` countermeasure orders and some UEL Working Mechanism announcements that
  never appear on the annual indexes.

MOFCOM publishes a stream of announcements rather than a structured snapshot of the
current lists. Monitoring must therefore detect notices that add, remove, suspend,
resume, stop, or amend measures and reconstruct cumulative state from those events.
The Export Control List is based on Article 28 of the Regulations on Export Control of
Dual-use Items (State Council Regulation No. 792), effective from 1 December 2024.

## Proposed flow

1. Fetch each index and follow pagination far enough to overlap the previous run.
2. Extract the notice URL, Chinese title, publication date, effective timestamp, and
   issuing authority.
3. Group URL aliases into candidate notice events, but compare their content before
   collapsing them. Preserve conflicting official copies for human review.
4. Rank unseen notices using authority-specific keywords and page text, including
   language indicating removal, suspension, resumption, amendment, or cancellation.
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

- Stable notice key, canonical official URL, and known official URL aliases.
- Authority, title, publication date, and effective timestamp.
- Announcement or decree number and legal basis.
- Event type: add, remove, suspend, resume, stop, amend, or informational.
- Stated numbered-target count and explicit legal-entity count when they differ.
- Content hash for each official copy and any conflict between those copies.
- Relationships to earlier events that the notice changes or supersedes.
- Review status: accepted, no designations, duplicate, out of scope, conflicting, or
  needs follow-up.
- Optional replacement URL, archived URL, and notes explaining the decision.

This state should be reviewable in Git. It must not use CSV row numbers or entity names
as identifiers because both can change independently of the source notice.

## Review output

A candidate report should include:

- Official URL and authority.
- Original Chinese title and publication date.
- Effective timestamp, when it differs from publication.
- Matched keyword or other discovery rule.
- Whether another known notice has the same title or date.
- Both target counts when numbered groups contain several legal entities.
- Any content difference between official URL aliases.
- Best-effort archived URL, if one was created.

Initially, produce one periodic report or GitHub issue rather than one issue per
notice. Do not automatically open a data PR: deciding who is designated, which
program applies, and when a measure starts often requires interpretation.

## Link rot and removals

Preserve the original official URL even when it stops resolving. A deleted page is not
evidence of delisting. Archiving should be best-effort and should not block ingestion;
an archive URL supplements rather than replaces the official citation.

Likewise, the monitor must not infer an end date from a notice disappearing. A measure
ends only when an official notice explicitly stops, cancels, or establishes the expiry
of that measure. Suspension and resumption are state changes, not end dates.

## Reliability checks

The monitor should fail visibly when an index cannot be parsed or changes structure.
A successful HTTP response with zero extracted links is an error, not a successful
empty run. Useful checks include:

- A minimum number of links extracted from each index.
- Stable parsing tests based on saved index and notice fixtures.
- URL-grouping tests for relative, redirected, mobile, and translated URLs.
- A test that official URL aliases with different content produce a conflict rather
  than being silently deduplicated.
- A test that previously reviewed false positives are not reported again.
- A test that out-of-scope government actions are recorded but do not become
  designation candidates.
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
