# Sanctions program

Sanctions programs are the specific government policies that form the legal basis for designating individuals, companies, vessels, or other entities as sanctioned. Each program defines a scope and a set of measures that the issuing authority imposes on the sanctioned target. See [What are sanctions programs?](/reference/faq/#sanctions-programs) for background.

Programs originate from the nature of the underlying policy mecahnism. They explain the legal and political basis for designations. While dataset metadata (see [metadata.md](metadata.md)) is data-centred and describes what the dataset contains and its limitations, program description is law-centred and provides more context on the mechanism behind designated entities.

In large consolidated lists such as `EU Financial Sanctions Files (FSF)`, `UN Security Council Consolidated Sanctions`, or `US OFAC Specially Designated Nationals (SDN) List`, a single dataset bundles multiple designation regimes. Programs distinguish between them, typically by geographical or thematic scope (counter-terrorism, cyber, etc.). In country-specific or subnational datasets, a single program covers the full legal instrument.

### Linking a programme to an entity

Sanctioned entities are linked to programmes via the `programId` property on their [`Sanction`](/reference/#schema.Sanction) records. This is handled through the `h.make_sanction()` helper (see [helpers.md](helpers.md)).

For datasets with a single programme, `h.make_sanction()` resolves the `programId` automatically from the hardcoded vaue (e.g. `PROGRAM_KEY = "US-BIS-DPL"`). For datasets covering multiple programmes (e.g. OFAC, EU FSF), pass the source's own programme key via `source_program_key`; the helper will resolve it to the canonical OpenSanctions key via `h.lookup_sanction_program_key()`.

The full set of programme metadata is published at `https://data.opensanctions.org/meta/programs.json`, updated multiple times per day.

### Program Metadata Basics

- `key` - Short unique identifier for code and cross-references, surfaced in the UI as e.g. `[EU-RUS]`. Convention: `{ISSUER}-{TARGET}` or `{ISSUER}-{SHORTNAME}`, e.g. `EU-HAM`, `SECO-IRAN`, `UN-SC1970`. Uppercase, alphanumeric with hyphens only.
- `title` - Official or near-official English title of the program. For non-English regimes (e.g. SECO), use a consistent English translation.
- `url` - Authoritative public-facing page at the issuing authority (SECO program page, EU sanctions map entry, UN Security Council committee page, etc.).
- `aliases` - List of strings. Optional. Alternative identifiers, legal citations, or short names, e.g. `Resolution 1970`, `UFLPA`, `SR 946.231.172.7`. Omit if none apply.
- `summary` - Plain-language description: who the program targets, why, and what measures it imposes. Two to four sentences. Must be consistent with the `measures` field.
- `dataset` - Related OpenSanctions dataset that ingests data from this program, e.g. `eu_fsf`, `ch_seco_sanctions`, `un_sc_sanctions`. Some programs are covered by multiple datasets but one is chosen here as the primary source.
- `issuer` - Identifier of the issuing authority from the controlled vocabulary, e.g. `eu_council`, `ch_seco`, `zz_unsc`, `us_ofac`, `us_dhs`, `ca_mfa`, `cz_mzv`.
- `target_territories` - List of strings. Optional. ISO 3166-1 alpha-2 codes (lowercase) for the targeted territories this program is linked to. Omit for programs that target persons regardless of geography (e.g. counter-terrorism lists).

### Measures

- `measures` - List of strings. Optional. One or more values from the [sanctions measures taxonomy](measures-taxonomy.md) describing what the program imposes. Verify against the legal instrument or the issuing authority's program page. Where a program transposes another regime (e.g. SECO transposing EU measures), reflect what the transposing authority implements. Valid values:
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

## Example

```yaml
title: EU Restrictive measures in view of the situation in the Democratic Republic of the Congo
key: EU-COD
url: https://sanctionsmap.eu/#/main/details/11/
summary: These sanctions target individuals and entities involved in the conflict,
  human rights abuses, and destabilizing activities in the Democratic Republic of
  the Congo. The measures include asset freezes, travel bans, and restrictions on
  trade to promote peace and stability in the region.
issuer: eu_council
dataset: eu_fsf
target_territories:
- cd
measures:
- Arms restrictions
- Asset freeze
- Travel ban
```
