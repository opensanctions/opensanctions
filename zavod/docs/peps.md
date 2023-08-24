# Developing crawlers for Politically Exposed Persons (PEPs)

If this is your first crawler, you may want to start with a basic crawler by 
following the [tutorial](tutorial.md), coming back here when you have one working.

Being classified as a PEP does not imply you have done anything wrong. However,
the concept is important because PEPs and members of their families should be
the subject of enhanced public scrutiny. This is also mandated by financial
crime laws in many countries. Read more about our [PEP data](https://www.opensanctions.org/pep/).

In addition to capturing general information about PEPs, a PEP crawler must

- Generate [Position entities](https://www.opensanctions.org/reference/#schema.Position) for each position in the dataset
- Generate [Occupancy entities](https://www.opensanctions.org/reference/#schema.Occupancy) representing the act of each person occupying a position for a period of time
- Add the `role.pep` [topic](https://www.opensanctions.org/reference/#type.topic) to each PEP Person entity
- Add the `role.rca` [topic](https://www.opensanctions.org/reference/#type.topic) to each relative or close associate, as well as the most appropriate entity to represent the relationship, e.g. [Family](https://www.opensanctions.org/reference/#schema.Family), [Associate](https://www.opensanctions.org/reference/#schema.Associate), or [UnknownLink](https://www.opensanctions.org/reference/#schema.UnknownLink)

## Creating Positions

The position name should ideally capture the position and its jurisdiction, but be no more specific than that.

Do

- use local preferred terminology
- include the role
- include the organisational body where needed
- include the specific geographic jurisdiction where relevant

Avoid

- the legislative term
- the constituency an elected official represents
- the country for national representatives

Examples

- Prefer `United States representative` over `Member of the House of Representatives` - while it's true that they're a member of the house of representatives, the common generic term is United States representative.
- Prefer `Member of the Landtag of Mecklenburg-Vorpommern` over `Member of the Landtag of Mecklenburg-Vorpommern, Germany` - the country is already captured as a property of the entity.
- Prefer `Member of the South African Parliament` over `Member of the 7th South African Parliament (2019-2024)` - there is currently no need to distinguish between different terms of the same position. Occupancies represent distinct periods when a person holds a given person. If the same position occurs twice in time, e.g. it was only possible to be `Minister of Electricity` up until 2015 and again from 2023, those can be distinguished sufficiently using the dissolution and inception properties.

Use the [`make_position`][zavod.helpers.make_position] helper to generate position entities consistently. 


