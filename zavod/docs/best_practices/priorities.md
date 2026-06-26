# Data collection priorities

With some data sources, extracting some attributes of people or companies
cleanly and reliably takes more effort than others, and not all attributes are
equally valuable to users.

**Every property in the schema is welcome.** If the source cleanly gives you a
property, capture it. You don't need to find it on a list below. The
authoritative, always-current set of available properties is the schema itself:

- `ftm ref schema Person` — people
- `ftm ref schema Company` / `ftm ref schema Organization` — companies and bodies
- `ftm ref schema Position` / `ftm ref schema Occupancy` — PEP positions and tenure

(Pipe to `jq -r '.properties[] | "\(.name) [\(.type)]"'` for a flat list.)

The lists below rank **where to spend effort** when a source makes some
attributes expensive to extract cleanly. Treat them as a guide to what to
prioritize, not as the set of properties available to you. To avoid going down
a rabbit hole, time-box the work and take a best-effort approach by these
priorities.

Aim for **complete coverage** — make sure all risk-associated entities (people,
companies, vessels, etc.) are included. But also **ensure accuracy**, e.g. make
sure not to mark someone as a PEP when they are not.

## Effort priorities (PEP and sanctions crawlers)

**Essential — get these right before anything else**

- Name(s) (see: [name cleaning and review framework](../extract/names.md))

**Essential when the source provides them**

- People: date of birth, place of birth, citizenship or nationality
- Official ID numbers (National ID for people, registration / VAT / tax number
  for companies, etc.)
- Other identifiers (see specifics in the schema, e.g. `innCode`, `wikidataId`)
- Country of birth, registration country (`Company:jurisdiction`)

**Worth a moderate effort**

- People: `biography`, `profession`, `gender`
- Companies/Organizations: `abbreviation`
- Companies/Organizations: date of registration/creation (often ambiguous)
- `sourceUrl` — only if it is a deep link to the specific company/person, not
  generic for the data source
- listing and effective dates (sanctions)
- company relationships
- person relationships
- addresses (except PEPs — see [below](#politically-exposed-persons))

**Worth capturing when cheap**

- anything else the source exposes cleanly. For people: `education`,
  `religion`, `title`, `website`, `wikipediaUrl`. For most entities: `notes`,
  `keywords`.

## Politically-exposed persons

PEP crawlers have additional required properties (`country`, `position`,
`occupancy`) and their own effort priorities. These live with the rest of the
PEP guidance: see [Properties to capture](../peps.md#properties-to-capture) in
the [guide for building PEP data crawlers](../peps.md).
