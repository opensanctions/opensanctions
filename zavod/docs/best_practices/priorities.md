# Data collection priorities

With some data sources, extracting some attributes of people or companies sufficiently cleanly/reliably can take more effort than others. Not all attributes are equally valuable to our users.

To avoid going too far down a rabbit hole or wasting effort, we recommend an approach of time-boxing the work on a crawler, and taking a best-effort approach according to the following priorities, categorised roughly by Essential, Should, Could and Won't.

Aim for **complete coverage** - make sure all risk-associated entities (people, companies, vessels, etc.) are included. But also **ensure accuracy**, e.g. make sure not to mark someone as a PEP when they are not.

## Generally (PEPs and Sanctions crawlers)

**Essential (bare minimum)**

- Name(s) (see: [name cleaning and review framework](../extract/names.md))

**Essential (when available)**

- People: Date of birth, place of birth, citizenship or nationality
- Official ID numbers (National ID for people, Registration number/VAT/tax for companies, etc)
- Other identifiers (See specifics in schemata, e.g. `innCode`, `wikidataId`)
- Country of birth, registration country (`Company:jurisdiction`)

**Should**

- Companies/Organizations: `abbreviation`
- Companies/Organizations: Date of registration/creation (often ambiguous)
- start/end dates - useful for [determining PEP status duration](../peps.md)
- listing and effective dates (sanctions)
- company relationships
- person relationships
- addresses (Except PEPs - see [below](#politically-exposed-persons))

**Could**

- sourceUrl - only if it is a deep link to the specific company/person, not generic for the data source.
- notes

## Politically-exposed persons

The PEP-specific property priorities (Must / Could / Won't) live with the rest of the
PEP guidance: see [Properties to capture](../peps.md#properties-to-capture) in the
[guide for building PEP data crawlers](../peps.md).
