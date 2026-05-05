# PEP Crawler Examples

## Pattern A: Known PEPs with `is_pep=True` (most common)

For sources that definitionally list PEPs (e.g. a national parliament):

```python
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, Any],
) -> None:
    person = context.make("Person")
    person.id = context.make_slug("mp", row.pop("id"))

    h.apply_name(
        person,
        first_name=row.pop("first_name"),
        last_name=row.pop("last_name"),
    )
    h.apply_date(person, "birthDate", row.pop("dob", None))
    person.add("gender", row.pop("gender", None))
    person.add("political", row.pop("party", None))

    # IMPORTANT: set ALL person props BEFORE calling make_occupancy.
    # make_occupancy reads birthDate/deathDate from the entity to determine PEP status.

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=row.pop("term_start", None),
        end_date=row.pop("term_end", None),
        propagate_country=True,
    )
    if occupancy is not None:
        context.emit(occupancy)
        # IMPORTANT: emit person AFTER make_occupancy — it adds role.pep to person.topics
        context.emit(person)

    context.audit_data(row, ignore=["photo_url"])


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of Parliament",
        country="xx",
        wikidata_id="Q...",
    )
    categorisation = categorise(context, position, is_pep=True)
    context.emit(position)

    data = context.fetch_json(context.data_url)
    for member in data["members"]:
        crawl_member(context, position, categorisation, member)
```

## Pattern B: Mixed dataset with `is_pep=None` (declaration-style)

For sources that list officials across many roles where some are PEP and some aren't.
PEP status is determined by the UI review workflow, not the crawler.

```python
def crawl_member(context: Context, row: dict[str, Any]) -> None:
    role = row.pop("role")
    position = h.make_position(context, name=role, country="fr")
    # is_pep=None: defers PEP determination to the UI
    categorisation = categorise(context, position, is_pep=None)

    if not categorisation.is_pep:
        return  # UI has not (yet) marked this position as PEP

    context.emit(position)

    person = context.make("Person")
    person.id = context.make_id(row.pop("id"))
    # ... set person props ...

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        status=OccupancyStatus.UNKNOWN,      # declaration != current office
        no_end_implies_current=False,         # no end date != still in office
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(person)
```

Key differences from Pattern A:
- `is_pep=None` — positions start uncategorised; the UI must mark them.
- `no_end_implies_current=False` — a declaration doesn't prove current office.
- `status=OccupancyStatus.UNKNOWN` — end date reliability is low.
- Position is created per-record (each unique role string becomes a position).

## Pattern C: Multi-position crawler with `is_pep=True`

For sources that list officials across known, enumerable position types:

```python
POSITIONS: dict[str, dict[str, Any]] = {
    "dail": {
        "name": "Member of Dail Eireann",
        "wikidata_id": "Q654291",
    },
    "seanad": {
        "name": "Senator of Seanad Eireann",
        "wikidata_id": "Q1396622",
    },
}

def crawl(context: Context) -> None:
    positions: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for key, config in POSITIONS.items():
        position = h.make_position(
            context,
            name=config["name"],
            country="ie",
            wikidata_id=config.get("wikidata_id"),
        )
        categorisation = categorise(context, position, is_pep=True)
        context.emit(position)
        positions[key] = (position, categorisation)

    # Then match each record to the right position + categorisation
```

## Current-only positions (no dates)

When the source only lists current officeholders with no term dates:

```python
occupancy = h.make_occupancy(
    context,
    person,
    position,
    categorisation=categorisation,
    is_current=True,        # no start_date/end_date needed
    propagate_country=True,
)
```

## Ambiguous end dates

When the source has unclear or unreliable end dates, use `OccupancyStatus.UNKNOWN`:

```python
from zavod.stateful.positions import OccupancyStatus

occupancy = h.make_occupancy(
    context,
    person,
    position,
    categorisation=categorisation,
    start_date=start,
    end_date=end,
    status=OccupancyStatus.UNKNOWN,
)
```

## Associates (for parliamentary staff/collaborators)

```python
assoc = context.make("Associate")
assoc.id = context.make_id(person.id, "associate", staff_name)
assoc.add("person", person)
assoc.add("associate", staff_entity)
assoc.add("relationship", "parliamentary assistant")
context.emit(assoc)
```
