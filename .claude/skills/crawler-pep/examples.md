# PEP Crawler Examples

## Pattern A: Known PEPs with `default_is_pep=True` (most common)

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
    name = row.pop("name")
    dob = row.pop("dob", None)
    person = context.make("Person")
    # name + DOB; anchor on a clean source ID instead when one exists. See entity_id.md.
    person.id = context.make_id(name, dob)

    person.add("name", name)  # name variants all go to `name`, not `alias`/`title`
    h.apply_date(person, "birthDate", dob)
    person.add("gender", row.pop("gender", None))
    person.add("political", row.pop("party", None))
    person.add("country", "xx")  # set explicitly when you omit citizenship
    person.add("sourceUrl", row.pop("profile_url", None))  # source-provided links only

    # IMPORTANT: set ALL person props BEFORE calling make_occupancy.
    # make_occupancy reads birthDate/deathDate from the entity to determine PEP status.

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=row.pop("term_start", None),
        end_date=row.pop("term_end", None),
    )
    if occupancy is not None:
        context.emit(occupancy)
        # IMPORTANT: emit person AFTER make_occupancy — it adds role.pep to
        # person.topics. Don't add role.pep yourself.
        context.emit(person)

    context.audit_data(row, ignore=["photo_url"])


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of Parliament",
        country="xx",
        wikidata_id="Q...",
        lang="eng",  # crawler-supplied names are always English
    )
    categorisation = categorise(context, position, default_is_pep=True)
    # Gate even with default_is_pep=True: the position may have been un-flagged
    # in the review UI, in which case emit nothing.
    if not categorisation.is_pep:
        return
    context.emit(position)

    data = context.fetch_json(context.data_url)
    for member in data["members"]:
        crawl_member(context, position, categorisation, member)
```

## Pattern B: Mixed dataset with `default_is_pep=None` (declaration-style)

For sources that list officials across many roles where some are PEP and some aren't.
PEP status is determined by the UI review workflow, not the crawler.

```python
def crawl_member(context: Context, row: dict[str, Any]) -> None:
    role = row.pop("role")  # source-supplied, in French
    position = h.make_position(
        context, name=role, country="fr", lang="fra", translate_name=True
    )
    # default_is_pep=None: defers PEP determination to the UI
    categorisation = categorise(context, position, default_is_pep=None)

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
- `default_is_pep=None` — positions start uncategorised; the UI must mark them.
- `no_end_implies_current=False` — a declaration doesn't prove current office.
- `status=OccupancyStatus.UNKNOWN` — end date reliability is low.
- Position is created per-record (each unique role string becomes a position).

### Subnational variant (per-municipality / per-region positions)

Same `default_is_pep=None` shape as Pattern B, but used for sources where each record names
a sub-national position (e.g. mayor of municipality X). Two extras:

- Pass the source-language label with `lang=` and `translate_name=True`.
- Pass `subnational_area=...` and **omit `wikidata_id`** — a Wikidata ID would
  collapse every municipality into the same entity.

```python
position = h.make_position(
    context,
    # Compose the whole name in the source language; translate_name produces the
    # English name in one pass and keys the ID on the untranslated original.
    name=f"{row.pop('MAN_LABEL')} de {commune_label}",
    country="lu",
    subnational_area=commune_label,           # NOT wikidata_id — per-locality
    lang="fra",
    translate_name=True,
)
categorisation = categorise(context, position, default_is_pep=None)
if not categorisation.is_pep:
    return
```

## Pattern C: Multi-position crawler with `default_is_pep=True`

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
            lang="eng",
        )
        categorisation = categorise(context, position, default_is_pep=True)
        context.emit(position)
        positions[key] = (position, categorisation)

    # Then match each record to the right position + categorisation
```

## Current-only positions (no dates)

When the source only lists current officeholders with no term dates, call
`make_occupancy` with no dates and it records a `current` occupancy:

```python
occupancy = h.make_occupancy(
    context,
    person,
    position,
    categorisation=categorisation,
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
