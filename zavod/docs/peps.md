# Developing crawlers for Politically Exposed Persons (PEPs)

If this is your first crawler, you may want to start with a basic crawler by 
following the [tutorial](tutorial.md), coming back here when you have one working.
You may also want to look at children of the [peps collection](https://github.com/opensanctions/opensanctions/blob/main/datasets/_collections/peps.yml)
to see common approaches.

Being classified as a PEP does not imply you have done anything wrong. However,
the concept is important because PEPs and members of their families should be
the subject of enhanced public scrutiny. This is also mandated by financial
crime laws in many countries. Read more about our [PEP data](https://www.opensanctions.org/pep/).

In addition to capturing general information about PEPs, a PEP crawler must

- Generate a [Position entity](https://www.opensanctions.org/reference/#schema.Position) for each position where a person has the kind of influence causing them to be defined as a PEP.
- Generate [Occupancy entities](https://www.opensanctions.org/reference/#schema.Occupancy) representing the act of each person occupying a position for a period of time.
- Add the `role.pep` [topic](https://www.opensanctions.org/reference/#type.topic) to each PEP Person entity.
- Add the `role.rca` [topic](https://www.opensanctions.org/reference/#type.topic) to each relative or close associate, as well as the most appropriate entity to represent the relationship, e.g. [Family](https://www.opensanctions.org/reference/#schema.Family), [Associate](https://www.opensanctions.org/reference/#schema.Associate), or [UnknownLink](https://www.opensanctions.org/reference/#schema.UnknownLink).

## Creating Positions

The [Position](https://www.opensanctions.org/reference/#schema.Position) `name` property should ideally capture the position and its jurisdiction, but be no more specific than that.

### Selecting a position name

Do

- use local preferred terminology
- include the role
- include the organisational body where needed
- include the specific geographic jurisdiction where relevant
- refer to [Wikidata EveryPolitician](https://www.wikidata.org/wiki/Wikidata:WikiProject_every_politician)
  for examples - specifically [position Q4164871](https://www.wikidata.org/wiki/Q4164871). 
  Much work has been done on defining positions in understandable and accurate
  ways here, and we plan on contributing our politician in the near future. 

Avoid

- including the legislative term
- including the constituency an elected official represents
- including the country for sub-national representatives

### Examples

- Prefer `United States representative` over `Member of the House of Representatives` - 
  while it's true that they're a member of the house of representatives, the 
  common generic term is United States representative.
- Prefer `Member of the Landtag of Mecklenburg-Vorpommern` over `Member of the 
  Landtag of Mecklenburg-Vorpommern, Germany` - the country is already captured
  as a property of the entity.
- Prefer `Member of the Hellenic Parliament` over `Member of the 17th Hellenic 
  Parliament (2015-202019)` - there is currently no need to distinguish between 
  different terms of the same position. Occupancies represent distinct periods 
  when a given person holds a position. If the same position occurs twice in time, 
  e.g. it was only possible to be `Minister of Electricity` up until 2015 and 
  again from 2023, those can be distinguished sufficiently using the dissolution 
  and inception properties rather than the name.

Use the [`make_position`][zavod.helpers.make_position] helper to generate position entities consistently. 

!!! info "Pro tip"
    It's perfectly fine to emit the same position over and over for each instance
    of a person holding that position, if that simplifies your code.

    It is often convenient to just create the person, all their positions, and 
    occupancies in a loop. You don't have to track created positions in your 
    crawler to avoid duplicates as long as the position `id` is consistent for
    each distinct position encountered. This will be the case if the values you
    pass [`make_position`][zavod.helpers.make_position] are consistent. The 
    export process will take care of deduplication of entities with consistent
    `id`s.

## Creating Occupancies

Occupanies represent the fact that a person holds or held a position for a given
period of time. If a person holds the same position numerous times, emit an
occupancy for each instance.

For most positions, someone holding a position becomes less and less significant over time.
It becomes less important to carry out anti money-laundering checks on people the
more time has passed since they held a position of influence which could enable
money laundering. We therefore only represent people as PEPs if a data source indicates
they hold the position now, or they left the position within the past 5 years.
In these cases the occupancy status should be `current` or `ended` respectively.

If it is unclear from the data or the data methodology of the source whether
a position is currently held or not, we consider someone a PEP if they have not
passed away, and they entered the position within the past 40 years. In this
case the occupancy status should be `unknown`.

### Only emit if they're a PEP

Only occupancies and positions should be emitted for instances where these
conditions are met. Persons should only be emitted if at least one occupancy
exists to indicate they meet our criteria for being considered a PEP.

The [`make_occupancy`][zavod.helpers.make_occupancy] helper will only return 
occupancies if they meet these conditions. You can use this to create occupancies,
automatically set the correct `status`, and determine whether the occupancy
meets our criteria and should be emitted.

### Example

```python
# ... looping over people in a province ...
if person_data.pop("death_date", None):
    return
person = context.make("Person")
source_id = person_data.pop("id")
person.add("country", "us")
person.add("name", person_data.pop("name"))
# ... more person properties ...

pep_entities = []
for role in person_data.pop("roles"):
    position = h.make_position(
        context,
        f"Member of the {province} Legislature",
        country="us",
        subnational_area=province
    )
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        True,
        start_date=role.get("start_date", None),
        end_date=role.get("end_date", None),
    )
    if occupancy:
        pep_entities.append(position)
        pep_entities.append(occupancy)

if pep_entities:
    person.add("topics", "role.pep")
    context.emit(person, target=True)
for entity in pep_entities:
    context.emit(entity)
```