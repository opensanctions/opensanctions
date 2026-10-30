# PEP crawl output integrity checks

Spot-check the crawl output with qsv against the run's `statements.pack`. Within
one dataset's pack, `entity_id` matches the ids that `Occupancy:holder` references. Each
check below should print nothing:

```bash
P=$(ls data/datasets/<dataset>/_artifacts/*/statements.pack | tail -1)

# 1. role.pep Person that never holds an Occupancy
comm -23 \
  <(qsv search -s prop '^Person:topics$' "$P" | qsv search -s value '^role\.pep$' | qsv select entity_id | qsv behead | sort -u) \
  <(qsv search -s prop '^Occupancy:holder$' "$P" | qsv select value | qsv behead | sort -u)

# 2. PEP Person with no country/citizenship/nationality
comm -23 \
  <(qsv search -s prop '^Person:topics$' "$P" | qsv search -s value '^role\.pep$' | qsv select entity_id | qsv behead | sort -u) \
  <(qsv search -s prop '^Person:(citizenship|country|nationality)$' "$P" | qsv select entity_id | qsv behead | sort -u)
```
