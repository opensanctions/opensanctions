# Upgrading crawlers

Changes to how crawlers are written, newest first. Each entry links to the rule it
changed and says how to recognise a crawler written before it.

## 2026-11-15: Sanction dates

Sanctions crawlers aim to fill `listingDate`, `startDate` and `modifiedAt` on every
Sanction. Call [`h.finalize_sanction`][zavod.helpers.finalize_sanction] before
emitting each Sanction. See [dates](sanctions/index.md#dates).

Signal: a Sanction emitted without `h.finalize_sanction`, or `listingDate` copied to
`startDate` with `adopt_statement`.
