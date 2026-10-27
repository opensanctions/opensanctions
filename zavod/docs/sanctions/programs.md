# Sanctions programs

Sanctions programs are the specific government policies that form the legal basis for designating individuals, companies, vessels, or other entities as sanctioned. While dataset metadata (see [metadata.md](../metadata.md)) is *data-centred* and describes what the dataset contains and its limitations, program descriptions are *law-centred* and provide more context on the mechanism behind designated entities. For a full description of what programs are, how they are structured, and the measures taxonomy, see the [public documentation](https://opensanctions.org/docs/programs).

Program field definitions live on the [`Program` model](https://github.com/search?q=repo%3Aopensanctions%2Fopensanctions+symbol%3AProgram&type=code).

## Key creation and program registration

Program keys are maintained as YAML files in `meta/programs/` in the repository (e.g. `meta/programs/EU-COD.yml`). The key field in the YAML is the canonical identifier. Before creating a new program, check the directory for an existing entry covering the same regime. If none exists, add a new file following the `{ISSUER}-{TARGET}` convention.

## Linking programs in crawler code

Sanctioned entities are linked to programs via the `programId` property on their [`Sanction`](https://opensanctions.org/reference/#schema.Sanction) records. This is handled through the `h.make_sanction()` helper (see [helpers.md](../helpers.md)).

Linking a program is the norm but not a requirement: a list that is one piece, with no identifiable sub-lists, may go without one. Many national lists implementing UNSC Resolution 1373 are like this.

For datasets with a single program, hardcode the key directly:
```python
PROGRAM_KEY = "US-BIS-DPL"
```
For datasets covering multiple programs, map the source's own program identifier through the `sanction.program` lookup with `h.lookup_sanction_program_key()`, and pass the raw identifier as `source_program_key`, which is recorded as the original value of `programId`. If the source key has no mapping, the lookup logs a warning and returns `None`.

```python
sanction = h.make_sanction(
    context,
    entity,
    key=program,
    program_name=program,
    source_program_key=program,
    program_key=h.lookup_sanction_program_key(context, program),
)
```

### Listings that impose no measures

Use [`h.make_risk`][zavod.helpers.make_risk] when the source names an entity without designating it: an advisory register, a watchlist of persons of interest, or a report to a legislature. It builds a [`Risk`](https://opensanctions.org/reference/#schema.Risk), flagging heightened exposure or scrutiny rather than a legally binding restriction.

`h.make_risk` takes the same arguments as [`h.make_sanction`][zavod.helpers.make_sanction] and fills the same properties from the dataset metadata and the resolved program key. Both helpers also add `programId` to the listed entity, so emit the entity after calling them (see [patterns](../best_practices/patterns.md#emit-an-entity-after-everything-that-writes-to-it)).
