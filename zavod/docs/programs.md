# Sanctions program

Sanctions programs are the specific government policies that form the legal basis for designating individuals, companies, vessels, or other entities as sanctioned. Each program defines a scope and a set of measures that the issuing authority imposes on the sanctioned target. See [What are sanctions programs?](/reference/faq/#sanctions-programs) for background.

Programs describe the legal and political basis for designations. While dataset metadata (see [metadata.md](metadata.md)) is data-centred and describes what the dataset contains and its limitations, program description is law-centred and provides more context on the mechanism behind designated entities.

In large consolidated lists such as `EU Financial Sanctions Files (FSF)`, `UN Security Council Consolidated Sanctions`, or `US OFAC Specially Designated Nationals (SDN) List`, a single dataset bundles multiple designation regimes. Programs distinguish between them, typically by geographical or thematic scope (counter-terrorism, cyber, etc.). In country-specific or subnational datasets, a single program covers the full legal instrument.

### Fields

- `key` - Short unique identifier for code and cross-references, surfaced in the UI as e.g. `[EU-RUS]`. Convention: `{ISSUER}-{TARGET}` or `{ISSUER}-{SHORTNAME}`, e.g. `EU-HAM`, `SECO-IRAN`, `UN-SC1970`. Uppercase, alphanumeric with hyphens only.
- `title` - Official or near-official English title of the program. For non-English regimes (e.g. SECO), use a consistent English translation.
- `url` - Authoritative public-facing page at the issuing authority (SECO program page, EU sanctions map entry, UN Security Council committee page, etc.).
- `aliases` - List of strings. Optional. Alternative identifiers, legal citations, or short names, e.g. `Resolution 1970`, `UFLPA`, `SR 946.231.172.7`. Omit if none apply.
- `summary` - Plain-language description: who the program targets, why, and what measures it imposes. Two to four sentences, consistent with the `measures` field.
- `dataset` - Related OpenSanctions dataset that ingests data from this program, e.g. `eu_fsf`, `ch_seco_sanctions`, `un_sc_sanctions`. Some programs are covered by multiple datasets but one is chosen here as the primary source.
- `issuer` - Identifier of the issuing authority from the controlled vocabulary, e.g. `eu_council`, `ch_seco`, `zz_unsc`, `us_ofac`, `us_dhs`, `ca_mfa`, `cz_mzv`.
- `target_territories` - List of strings. Optional. ISO 3166-1 alpha-2 codes (lowercase) for the targeted territories this program is linked to. Omit for programs that target persons regardless of geography (e.g. counter-terrorism lists).
- `measures` — one or more values from the [sanctions measures taxonomy](measures-taxonomy.md) describing what the program imposes. Use the legal instrument or the issuing authority's program page as your source of truth. For programs that transpose another regime (e.g. SECO transposing EU measures), verify against the transposing authority's own legal instrument. The measures may match the original, cover only a subset, or extend it.

## Implementation

### Measures

Valid values:
  - `Aid suspension`
  - `Arms embargo`
  - `Arms restrictions`
  - `Asset freeze`
  - `Debarment`
  - `Export control`
  - `Financial restrictions`
  - `Import restrictions`
  - `Investment ban`
  - `Services ban`
  - `Prohibition to satisfy claims`
  - `Sectoral sanctions`
  - `Transportation restrictions`
  - `Travel ban`

### Key creation and program registration

Program keys are maintained as YAML files in `meta/programs/` in the repository (e.g. `meta/programs/EU-COD.yml`). The key field in the YAML is the canonical identifier. Before creating a new program, check the directory for an existing entry covering the same regime. If none exists, add a new file following the `{ISSUER}-{TARGET}` convention.

### Linking programs in crawler code

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
