# Wikidata

OpenSanctions imports [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) through a PEP-position crawler and an enricher, and publishes a small set of properties back to Wikidata under human supervision.

Wikidata reaches OpenSanctions in two ways: a [crawler](tutorial.md) that imports persons who hold any of a set of Wikidata positions categorized as [Politically Exposed Person positions](https://opensanctions.org/pep), and the [Wikidata enricher](https://www.opensanctions.org/datasets/wikidata/). A small selection of properties is also published back to Wikidata through an interactive process that a human supervises in full.

## Reconciling a dataset against Wikidata

The `zavod wikidata-reconcile` command matches the persons in a dataset against Wikidata and prepares edits for the ones it links. It does not write to Wikidata directly. Confirmed `<os_entity_id> ↔ QID` links are recorded in the [resolver](https://www.opensanctions.org/docs/identifiers/), and the run writes a [QuickStatements](https://quickstatements.toolforge.org/) batch file that an operator reviews and runs in the QuickStatements web UI.

The command handles the `Person` schema only. It is built for the parliament/PEP use case, where a large fraction of members already have a Wikidata item.

### Running wikidata-reconcile

Run the command against one or more dataset paths:

```bash
zavod wikidata-reconcile \
  --rebuild-store \
  datasets/de/abgeordnetenwatch/de_abgeordnetenwatch.yml
```

The command always runs in review mode. Each unlinked person is shown against its ranked Wikidata search candidates, and you decide per person: confirm a match, record no-match, mark unsure, create a new item, or skip. Candidates are fetched, scored, and sorted up front (watch the log), so the review runs in memory without per-screen network stalls.

For each person, the run produces QuickStatements commands:

- **Linked and confirmed persons.** A person already linked to a QID (the resolver canonical is a QID, or the entity carries a `wikidataId`), or confirmed in review, is diffed against its Wikidata item and enriched with the properties the item is missing: birth date, citizenship (`P27`), gender (`P21`), positions held (`P39`), and names as aliases.
- **Create decisions.** A create produces a new-item block (label, description, aliases, and core statements) for the operator to run.

On exit, the commands are written to a single `.qs` batch that you upload in the QuickStatements UI. By default the batch lands at `<dataset state path>/wikidata-reconcile.qs`; pass `-o/--output` to change the path. Newly created items pick up their `entity ↔ QID` link on a later reconciliation pass, once they exist and search finds them.

### Options

- `-r/--rebuild-store` — re-sync the entity store before reconciling.
- `--aliases/--no-aliases` — include alternate names as search aliases. On by default.
- `-a/--algorithm` — scoring algorithm used to rank candidates. Defaults to the ER matcher.
- `-o/--output` — path for the QuickStatements output file.

References on each statement use the entity's own `sourceUrl`, falling back to the dataset's `url`. The dataset's `updated_at` provides the retrieved-on date. A freshly crawled local dataset often has no `updated_at` yet, so the retrieved-on qualifier is omitted in that case.

### Notes and limitations

- **Persons only.** Positions are emitted as `P39` only when the `Position` carries a Wikidata QID; no Wikidata lookup of positions is performed.
- **Full export required for positions.** `P39` emission needs the `Occupancy` and `Position` entities, so a persons-only slice does not carry the occupancies.
- **Gender.** Emitted only for unambiguous `male` or `female` values.
- **Files only.** QuickStatements API submission is out of scope; the command writes files.
