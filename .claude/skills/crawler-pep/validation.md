# PEP crawl output integrity checks

Spot-check the crawl output with qsv against `data/datasets/<dataset>/statements.pack`.
The `prop` column is `Schema:property`, so entity type is recoverable; within one
dataset's pack `entity_id` matches the ids that `Occupancy:holder`/`post` reference (this
is pre-resolution crawl output). Each integrity check below should print nothing:

```bash
P=data/datasets/<dataset>/statements.pack

# Entity counts — sanity-check against the assertions block
for s in Person Position Occupancy; do
  echo "$s: $(qsv search -s prop "^${s}:id\$" "$P" | qsv behead | wc -l)"
done

# 1. Occupancy.post referencing a Position that wasn't emitted
comm -23 \
  <(qsv search -s prop '^Occupancy:post$' "$P" | qsv select value     | qsv behead | sort -u) \
  <(qsv search -s prop '^Position:'       "$P" | qsv select entity_id | qsv behead | sort -u)

# 2. Occupancy.holder referencing a Person that wasn't emitted
comm -23 \
  <(qsv search -s prop '^Occupancy:holder$' "$P" | qsv select value     | qsv behead | sort -u) \
  <(qsv search -s prop '^Person:'           "$P" | qsv select entity_id | qsv behead | sort -u)

# 3. role.pep Person that never holds an Occupancy
comm -23 \
  <(qsv search -s prop '^Person:topics$' "$P" | qsv search -s value '^role\.pep$' | qsv select entity_id | qsv behead | sort -u) \
  <(qsv search -s prop '^Occupancy:holder$' "$P" | qsv select value | qsv behead | sort -u)

# 4. PEP Person with no country/citizenship/nationality (make_occupancy no longer
#    back-fills country from the position, so this must be set explicitly)
comm -23 \
  <(qsv search -s prop '^Person:topics$' "$P" | qsv search -s value '^role\.pep$' | qsv select entity_id | qsv behead | sort -u) \
  <(qsv search -s prop '^Person:(citizenship|country|nationality)$' "$P" | qsv select entity_id | qsv behead | sort -u)
```
