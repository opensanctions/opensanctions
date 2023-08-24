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



