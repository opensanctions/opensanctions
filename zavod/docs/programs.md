# Sanctions programs

Sanctions programs are the specific government policies that form the legal basis for designating individuals, companies, vessels, or other entities as sanctioned. While dataset metadata (see [metadata.md](metadata.md)) is *data-centred* and describes what the dataset contains and its limitations, program descriptions are *law-centred* and provide more context on the mechanism behind designated entities. For a full description of what programs are, how they are structured, and the measures taxonomy, see the [public documentation](https://opensanctions.org/docs/programs).

Program field definitions live on the [`Program` model](https://github.com/search?q=repo%3Aopensanctions%2Fopensanctions+symbol%3AProgram&type=code).

## Key creation and program registration

Program keys are maintained as YAML files in `meta/programs/` in the repository (e.g. `meta/programs/EU-COD.yml`). The key field in the YAML is the canonical identifier. Before creating a new program, check the directory for an existing entry covering the same regime. If none exists, add a new file following the `{ISSUER}-{TARGET}` convention.

## Linking programs in crawler code

Sanctioned entities are linked to programs via the `programId` property on their [`Sanction`](https://opensanctions.org/reference/#schema.Sanction) records. This is handled through the `h.make_sanction()` helper (see [helpers.md](helpers.md)).

For datasets with a single program, hardcode the key directly:
```python
PROGRAM_KEY = "US-BIS-DPL"
```
For datasets covering multiple programs, pass the source's own program identifier via `source_program_key`. The helper resolves it to the canonical OpenSanctions key via `h.lookup_sanction_program_key()`. If the source key has no mapping, the helper logs a warning and returns `None`.
