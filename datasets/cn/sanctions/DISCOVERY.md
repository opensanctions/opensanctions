---
description: Plan for finding historical and newly published Chinese sanctions notices across official authorities.
date: 2026-06-29
tags: [cn_sanctions, discovery, backfill, official-sources]
---

# Discovering Chinese sanctions notices

## Objective

Build a reproducible inventory of official notices that add, remove, suspend, amend,
or otherwise change Chinese sanctions and export-control measures. Discovery should
answer two separate questions:

1. Which historical notices are missing from `sanctions.csv`?
2. Which relevant notices have appeared since the last review?

The unit of discovery is a notice, not an entity. One notice may designate many
entities, and the same entity may appear in several notices. Entity merging happens
later and should not affect whether a notice is reviewed.

## Source policy

Designation rows require an official government source. Search engines, commercial
screening providers, research institutes, and news reporting are useful discovery
leads, but they do not replace the official notice.

Prefer the issuing authority's page. An official `gov.cn` mirror or government gazette
can be used to recover a notice that has disappeared, while retaining the original URL
when known. A dead source page is not evidence that a measure has ended.

Record official actions that match discovery terms but fall outside the dataset scope,
such as a blocking measure protecting Chinese companies. Marking them `out of scope`
prevents repeated review without turning them into designation candidates.

## Ministry of Foreign Affairs

### Primary indexes

- [MFA anti-sanctions lists and measures](https://www.mfa.gov.cn/web/wjb_673085/zfxxgk_674865/gknrlb/fzcqdcs/)
- MFA news pages under both `/web/wjbxw_new/` and `/wjbxw_new/`
- Legacy English pages on `fmprc.gov.cn`

The dedicated index is the strongest starting point, but it is not a complete archive
of every informal or embassy-announced measure. Notice URLs also move between MFA
site structures. Group aliases as one candidate event, but compare their content before
collapsing them: official copies can contain substantive differences. Preserve both
versions and flag the event when they disagree.

### Discovery methods

1. Enumerate every link in the dedicated index and compare it with known source URLs.
2. Search MFA news for `外交部令` and enumerate decree numbers. A gap is a review lead,
   not proof that a sanctions decree is missing.
3. Search titles and page text for:
   - `采取反制措施`
   - `反制清单`
   - `反制裁清单`
   - `反外国制裁法`
   - `暂停`, `变更`, `取消`, and `解除` together with `反制措施`
4. Search the legacy and current domains separately:
   - `site:mfa.gov.cn "采取反制措施"`
   - `site:mfa.gov.cn "反外国制裁法"`
   - `site:fmprc.gov.cn countermeasures`
   - `site:fmprc.gov.cn "Anti-Foreign Sanctions Law"`
5. Use spokesperson remarks as cross-checks. They often mention the number of targets
   and link to the actual decision, but the decision remains the designation source.

### Embassy and consulate coverage

Some measures appear first or only on a Chinese embassy or consulate website. Build a
domain list from the MFA directory and run the same Chinese and English searches over
those domains. Results need manual review because embassy sites also reproduce central
MFA content.

An initial comparison already indicates missing coverage: the central index contains
a December 2025 decision covering 20 companies and 10 executives and a March 2026
decision concerning Furuya Keiji, neither of which is currently represented in the
CSV. These should be the first backfill test cases.

## Ministry of Commerce

### Primary indexes

- [Security and Control Bureau regulations and normative documents](https://aqygzj.mofcom.gov.cn/flzc/gzjgfxwj/index.html)
- [MOFCOM policy releases](https://www.mofcom.gov.cn/zwgk/zcfb/)
- Annual MOFCOM announcement archives under `政策发布 > 部令公告`

MOFCOM publishes announcements rather than a structured current list. Discovery must
therefore reconstruct list state from a sequence of events. The same announcement can
also be exposed under several official URL layouts, including `/zwgk/zcfb/art/` and
`/zcfb/blgg/gg/`. Treat these as copies only after their bodies and attachments have
been compared.

### Notice families

Search for additions and state changes across all four relevant families:

| Family | Addition terms | State-change terms |
| --- | --- | --- |
| Export Control List | `列入出口管制管控名单`, `出口管制管控名单` | `移出`, `暂停`, `调整`, `取消` |
| Export Control Watch List | `列入关注名单`, `关注名单` | `移出关注名单`, `调整`, `取消` |
| Unreliable Entity List | `列入不可靠实体清单`, `不可靠实体清单工作机制` | `移出`, `暂停`, `调整`, `取消有关措施` |
| Countermeasures | `采取反制措施`, `反制清单` | `暂停`, `变更`, `取消反制措施` |

Do not restrict matching to notice titles. Some notices describe removals or narrower
changes only in the body or attachment.

### Discovery methods

1. Crawl the Security and Control Bureau index through all available pagination.
2. Enumerate annual MOFCOM announcements and filter on issuing unit, title, body text,
   and legal basis. Announcement-number gaps are candidates for inspection only.
3. Search the main MOFCOM domain for each notice-family phrase and its state-change
   variants.
4. Follow attachments. The authoritative name list may be inline HTML, PDF, Word, or a
   separate download.
5. Compare the notice's stated entity count with the number of extracted rows.
6. Record all official aliases of a notice URL so a redesigned site does not generate
   false new candidates.

The Unreliable Entity List is sometimes published in the name of the `不可靠实体清单工作机制`
rather than MOFCOM itself. Discovery must match that issuer phrase as well as the
ministry name.

## Taiwan Affairs Office

### Primary paths

- [Measures against Taiwan independence](https://www.gwytb.gov.cn/zccs/zccs_61195/cjtdwgfz/bt/)
- Taiwan Affairs Office press-conference and spokesperson archives
- Current formal-list pages for diehard separatists and accomplices

The thematic index is useful but has been intermittently unavailable and notice titles
are inconsistent. Current-list snapshots can reveal targets whose original designation
notice is missing; retain that distinction rather than inventing a start date. Search
page text as well as titles for:

- `“台独”顽固分子`
- `“台独”打手帮凶`
- `依法实施惩戒`
- `公布清单`
- `关联机构` and `资助` as broader review leads

Distinguish people explicitly placed on the two named lists from associated
organizations and donors. The latter may be subject to measures without belonging to
the same formal program.

## Other central authorities

The Anti-Foreign Sanctions Law permits relevant State Council departments to impose
countermeasures, so discovery should not assume that MFA and MOFCOM are exhaustive.
Use the central government policy index and State Council Gazette to search for the
same legal and action phrases across department domains:

- `反外国制裁法` with `决定`, `名单`, or `措施`
- `列入` with `清单`
- `暂停`, `变更`, or `取消` with `反制`

When another authority appears, add its official publication index and domain to the
source inventory before extracting data. Do not add speculative authorities merely
because their mandate could support sanctions.

## Historical backfill

### Build the notice inventory

Seed the inventory with every distinct `Source URL` currently in `sanctions.csv`, then
add every notice found in the official indexes. Track at least:

- Stable notice key.
- Official URL and known official URL aliases.
- Authority and issuing unit.
- Original title, publication date, and effective timestamp.
- Announcement or decree number.
- Legal basis and inferred notice family.
- Event type: add, remove, suspend, resume, stop, amend, replace, or informational.
- Stated numbered-target count and explicit legal-entity count.
- Content hash for each official copy and any conflict between copies.
- The prior event changed or superseded by this event.
- Review status, including accepted, irrelevant, out of scope, conflicting, and needs
  follow-up.
- Reviewer notes.

The stable key should be based on authority plus announcement/decree number when one
exists, with a normalized URL fallback. It must not depend on CSV row numbers.
Different content under apparent URL aliases must remain separate versions of the
event until a reviewer resolves the conflict.

### Reconcile against the CSV

For each accepted notice:

1. Match official URL aliases to existing source URLs.
2. Compare both the numbered-target and legal-entity counts with rows attributed to
   it. Record the modelling decision when one numbered group contains several legal
   entities.
3. Check whether every listed target is represented, without treating repeated targets
   as errors.
4. Check date, authority, program, names, aliases, addresses, and event direction.
5. Submit one focused PR per notice or small batch of closely related notices.

Work newest-to-oldest within each authority so that current monitoring becomes useful
quickly, then continue back to the start of each regime: informal countermeasures,
the 2020 UEL framework, the 2021 Anti-Foreign Sanctions Law, and the dual-use export
control regime.

## Continuous discovery

After the backfill inventory is stable:

1. Poll primary indexes daily and retain a bounded pagination overlap.
2. Hash index entries and notice bodies to detect edits as well as new URLs.
3. Snapshot current formal-list pages and compare their membership over time.
4. Run broader official-domain searches periodically to catch orphan and embassy pages.
5. Produce one review queue containing new, changed, and conflicting notices and list
   snapshots.
6. Mark every candidate accepted, irrelevant, out of scope, duplicate, conflicting, or
   needing follow-up so false positives are not repeatedly raised.

Automated discovery should never directly alter `sanctions.csv`. Extraction and
classification remain reviewed PR work.

## Completeness checks

- Every official index yields at least a plausible minimum number of links.
- Every announcement/decree sequence gap has been inspected or explained.
- Every notice's numbered-target and legal-entity counts match the reviewed extraction
  or have an explicit modelling decision.
- Official URL aliases collapse only after their content has been compared.
- Search-engine results do not reveal official notices absent from the inventory.
- Known removals, suspensions, and cancellations have a reviewed state transition.
- Current formal-list snapshots do not contain unexplained members.
- Secondary coverage lists do not reveal unexplained official-source gaps.

## Initial checkpoints

1. Inventory and reconcile the current MFA anti-sanctions index.
2. Inventory MOFCOM announcements from 2024 onward across ECL, ECWL, UEL, and
   countermeasures.
3. Backfill the Taiwan Affairs Office thematic and press-conference archives.
4. Add embassy-domain and cross-department searches.
5. Convert the proven discovery rules into the monitor described in `AUTOMATION.md`.

Stop after each authority inventory and report the number of notices found, accepted,
missing, duplicated, and unresolved before beginning the next source family.
