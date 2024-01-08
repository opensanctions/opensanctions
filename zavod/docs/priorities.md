# Data priorities

With some data sources, extracting some attributes of people or companies sufficiently cleanly/reliably can take more effort than others. Not all attributes are equally valuable to our users.

To avoid going too far down a rabbit hole or wasting effort, we recommend an approach of time-boxing the work on a crawler, and taking a best-effort approach according to the following priorities, categorised roughly by Essential, Should, Could and Won't.

Aim for **complete coverage** - make sure all [targets](https://www.opensanctions.org/docs/glossary/#targets) are included. But also **ensure accuracy**, e.g. make sure not to mark someone as a PEP when they are not.

## Generally

**Essential (bare minimum)**

- Name(s)

**Essential (when available)**

- People: Date of birth, nationality
- Companies/Organizations: Date of registration/creation
- Official ID numbers (National ID for people, Registration number for companies, etc)
- Other identifiers (See specifics in schemata, e.g. `innCode`, `wikidataId`)
- Country of birth, registration country (`Company:jurisdiction`)

**Should**

- start/end dates - useful for determining PEP status duration
- listing dates (sanctions)
- company relationships
- person relationships
- addresses (Except PEPs - see [below](#politically-exposed-persons))

**Could**

- sourceUrl
- notes

## Politically-exposed persons

**Must**

- country (occasionally multiple apply to one position, e.g. *Ambassador of Palestine to Germany*)
- position (of a person)
- occupancy (relating a person to the position(s) they hold/held) - focus on current positions before worrying about historical.

**Could**

- [Position:subnationalArea](https://www.opensanctions.org/reference/#schema.Position)

**Won't - don't extract**

- Addresses (not needed, and privacy concern)