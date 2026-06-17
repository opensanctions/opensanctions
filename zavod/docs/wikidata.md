# Wikidata

We import [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) to
OpenSanctions in two ways: using a [crawler](tutorial.md) which
imports persons who have held any of a set of wikidata positions we have categorised as
[Politically Exposed Person positions](https://opensanctions.org/pep), and using
our [Wikidata Enricher](https://www.opensanctions.org/datasets/wikidata/).

We also occasionally publish data for a small selection of properties to Wikidata.
The current publishing process is interactive and completely supervised by a
human.

## Reconciling a dataset against Wikidata

The `zavod wikidata-reconcile` command matches the persons in a dataset against
Wikidata and prepares edits for the ones it links. It does **not** write to
Wikidata directly: confirmed `<os_entity_id> ↔ QID` links are recorded in the
[resolver](https://www.opensanctions.org/docs/identifiers/), and the run writes a
[QuickStatements](https://quickstatements.toolforge.org/) batch file that an
operator reviews and runs in the QuickStatements web UI. There is no OAuth,
pywikibot, or API write path — the human in the loop runs the batch.

The tool is person-only (the `Person` schema) and built for the parliament/PEP
use case, where a large fraction of members already have a Wikidata item.

### Running `wikidata-reconcile`

```
zavod wikidata-reconcile \
  --rebuild-store \
  datasets/de/abgeordnetenwatch/de_abgeordnetenwatch.yml
```

The command always runs in review mode: each unlinked person is shown against its
ranked Wikidata search candidates, and you decide per person — **confirm** a match,
**no-match**, **unsure**, **create** a new item, or **skip**. Candidates are
fetched, scored and sorted up front (watch the log), so the review itself runs in
memory without per-screen network stalls.

For each person the run produces QuickStatements commands:

- **Linked persons** (resolver canonical is a QID, or the entity carries a
  `wikidataId`) and **confirmed** matches are diffed against their Wikidata item
  and enriched — adding properties the item is missing (birth date, citizenship
  `P27`, gender `P21`, positions held `P39`, names as aliases, …).
- **Create** decisions emit a new-item block (label, description, aliases, core
  statements) for the operator to run.

On exit the commands are written to a single `.qs` batch
(default: `<dataset state path>/wikidata-reconcile.qs`, override with `-o/--output`)
that you upload in the QuickStatements UI. Newly created items pick up their
entity↔QID link on a later reconciliation pass, once they exist and search finds
them.

### Options

- `-r/--rebuild-store` — re-sync the entity store before reconciling.
- `--aliases/--no-aliases` — include alternate names as search aliases (on by default).
- `-a/--algorithm` — scoring algorithm used to rank candidates (defaults to the
  ER matcher).
- `-o/--output` — QuickStatements output path.

References on each statement use the entity's own `sourceUrl`, falling back to the
dataset's `url`; the dataset's `updated_at` is used as the retrieved-on date. A
freshly crawled local dataset often has no `updated_at` yet, so the retrieved-on
qualifier is simply omitted in that case.

### Notes and limitations

- Person entities only; positions are emitted as `P39` only when the `Position`
  carries a Wikidata QID (no Wikidata lookup of positions is performed).
- A full dataset export (including the `Occupancy` and `Position` entities) is
  required for `P39` position emission — a persons-only slice will not carry the
  occupancies.
- Gender is emitted only for unambiguous `male`/`female` values.
- QuickStatements API submission is out of scope: the tool writes files only.
