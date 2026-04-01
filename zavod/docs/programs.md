# Sanctions programs

Sanctions programs are the specific government policies that form the legal basis for designating individuals, companies, vessels, or other entities as sanctioned. For a full description of what programs are, how they are structured, and the measures taxonomy, see the [public documentation](https://opensanctions.org/docs/programs).

Program field definitions live on the `Program` model in `zavod/stateful/programs.py`.

## Key creation and program registration

Program keys are maintained as YAML files in `meta/programs/` in the repository (e.g. `meta/programs/EU-COD.yml`). The key field in the YAML is the canonical identifier. Before creating a new program, check the directory for an existing entry covering the same regime. If none exists, add a new file following the `{ISSUER}-{TARGET}` convention.

## Linking programs in crawler code

Sanctioned entities are linked to programs via the `programId` property on their [`Sanction`](/reference/#schema.Sanction) records. This is handled through the `h.make_sanction()` helper (see [helpers.md](helpers.md)).

For datasets with a single program, hardcode the key directly:
```python
PROGRAM_KEY = "US-BIS-DPL"
```
For datasets covering multiple programs, pass the source's own program identifier via `source_program_key`. The helper resolves it to the canonical OpenSanctions key via `h.lookup_sanction_program_key()`. If the source key has no mapping, the helper logs a warning and returns `None`.

## Example

```yaml
title: EU Restrictive measures in view of the situation in the Democratic Republic of the Congo
key: EU-COD
url: https://sanctionsmap.eu/#/main/details/11/
summary: Targets individuals and entities responsible for serious human rights violations,
  obstruction of the electoral process, and sustaining the armed conflict in the DRC.
  Designated persons are subject to an asset freeze and travel ban. The regime also
  implements the UN arms embargo against those supplying or transferring arms in violation
  of UN Security Council resolutions.
issuer: eu_council
dataset: eu_fsf
target_territories:
- cd
measures:
- Arms restrictions
- Asset freeze
- Prohibition to satisfy claims
- Travel ban
```
