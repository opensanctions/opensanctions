# PEP crawler examples

Worked code for the rules in the [PEP guide](index.md). Start from the reference crawler;
the other sections show how it changes for other kinds of source.

## Reference crawler

A crawler for Luxembourg's Chamber of Deputies: one position, a current-only roster
published as a CSV, and per-person start dates. It's based on `lu_chamber`, but work from
this version: the crawler in the repository predates these conventions.

```python
import csv
from typing import Any
from urllib.parse import urljoin

from rigour.mime.types import CSV
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h


def crawl_row(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, Any],
) -> None:
    first_name = row.pop("pph_prenom")
    last_name = row.pop("pph_nom")
    dob = row.pop("pph_date_naissance")

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, dob)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    h.apply_date(person, "birthDate", dob)
    person.add("gender", row.pop("per_titre"))
    person.add("political", row.pop("rattachement_abrv"))
    person.add("email", row.pop("email"))
    # Deputies must be Luxembourgish: Constitution of Luxembourg (2023), Art. 64(1)
    # and (2). https://www.venice.coe.int/webforms/documents/default.aspx?pdffile=CDL-REF(2023)013-f
    person.add("citizenship", "lu")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=row.pop("date_debut_depute"),
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", row.pop("derniere_circonscription"))
    context.emit(occupancy)
    context.emit(person)

    context.audit_data(
        row,
        ignore=[
            # "Groupe politique"/"Sensibilité politique": the group's parliamentary
            # standing, which has no FollowTheMoney property.
            "rattachement_type",
            # A home address for some members; private addresses are not extracted.
            "address",
            # Phone numbers are not extracted for PEPs.
            "phone_ext",
            "phone_mobile",
        ],
    )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Deputy of the Chamber of Deputies of Luxembourg",
        wikidata_id="Q21328592",
        country="lu",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    # The catalog republishes the CSV daily under a new timestamped URL and deletes
    # the old one, so resolve the current file from the dataset's metadata on every
    # run, uncached.
    dataset = context.fetch_json(
        urljoin(
            context.data_url,
            "la-liste-des-deputes-actifs-a-la-chambre-des-deputes-du-luxembourg/",
        )
    )
    csv_urls = [r["url"] for r in dataset["resources"] if r["format"] == "csv"]
    assert len(csv_urls) == 1, csv_urls

    path = context.fetch_resource("deputies.csv", csv_urls[0])
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    # The source exports as Windows-1252, not UTF-8. Detection is unreliable here:
    # byte 0xe8 is valid in several single-byte codepages, so a detector mistakes the
    # French/Luxembourgish text for cp1250 and yields mojibake.
    with open(path, encoding="cp1252") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, position, categorisation, row)
```

The date formats and the gender values live in the dataset YAML, not in code:

```yaml
data:
  url: https://data.public.lu/api/1/datasets/
  format: CSV
dates:
  formats: ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]

lookups:
  type.gender:
    options:
      - match: Monsieur
        value: male
      - match: Madame
        value: female
```

Two things first drafts most often get wrong, and this gets right:

- **The row loop feeds `crawl_row` directly**: no discovery helper collecting IDs first,
  no repeated requests "to be safe", no hand-written row-count check. The YAML
  `assertions` own totals.
- **Each workaround carries its reason** (the uncached catalog lookup, the `cp1252`
  encoding), so a reviewer can tell necessary code from machinery.

## Mixed dataset

A source that lists officials across many roles, only some of them PEPs, such as an
asset-declaration register. See [Categorizing positions](index.md#categorizing-positions).

```python
def crawl_member(context: Context, row: dict[str, Any]) -> None:
    position = h.make_position(
        context, name=row.pop("role"), country="fr", lang="fra", translate_name=True
    )
    categorisation = categorise(context, position, default_is_pep=None)
    if not categorisation.is_pep:
        return

    person = context.make("Person")
    person.id = context.make_id(row.pop("id"))
    # ... set person props ...

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        # A declaration gives no dates to derive a status from.
        status=OccupancyStatus.UNKNOWN,
        no_end_implies_current=False,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(position)
        context.emit(person)
```

### Subnational positions

The same shape for sources where each record names a sub-national position, such as the
mayor of a municipality. Pass the locality as `subnational_area`, and no `wikidata_id`:

```python
position = h.make_position(
    context,
    name=f"{row.pop('MAN_LABEL')}, {commune_label}",
    country="lu",
    subnational_area=commune_label,
    lang="fra",
    translate_name=True,
)
categorisation = categorise(context, position, default_is_pep=None)
if not categorisation.is_pep:
    return
```

## Several positions

See [Several positions in one crawler](index.md#several-positions-in-one-crawler). This is
the code shape for the lookup case.

```yaml
lookups:
  position:
    normalize: true
    options:
      - match: House of Representatives
        name: United States representative
        wikidata_id: Q13218630
        topics: [gov.national, gov.legislative]
      - match: Senate
        name: United States senator
        wikidata_id: Q4416090
        topics: [gov.national, gov.legislative]
      - match: Territorial delegation
        value: null
```

```python
res = context.lookup("position", row.pop("chamber"), warn_unmatched=True)
if res is None or res.name is None:
    return
position = h.make_position(
    context,
    name=res.name,
    country="us",
    topics=res.topics,
    wikidata_id=res.wikidata_id,
    lang="eng",
)
categorisation = categorise(context, position)
if not categorisation.is_pep:
    return
occupancy = h.make_occupancy(context, person, position, categorisation=categorisation)
if occupancy is not None:
    context.emit(occupancy)
    context.emit(position)
    context.emit(person)
```

### Presiding offices

The membership is built once in `crawl()`, as in the reference crawler. Here the
source's `role` field is empty for plain members and names the presiding office
otherwise, so the lookup maps each role to its office, built per person:

```yaml
lookups:
  position:
    required: true
    options:
      - match: Speaker
        name: Speaker of the Parliament of Examplia
        wikidata_id: Q...
        topics: [gov.national, gov.legislative]
```

```python
def crawl_member(
    context: Context,
    member: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, Any],
) -> None:
    person = context.make("Person")
    # ... set person props ...

    occupancy = h.make_occupancy(
        context, person, member, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)

    role = row.pop("role")
    if role is not None:
        res = context.lookup("position", role)
        assert res is not None, role
        office = h.make_position(
            context,
            name=res.name,
            country="xx",
            topics=res.topics,
            wikidata_id=res.wikidata_id,
            lang="eng",
        )
        office_categorisation = categorise(context, office)
        if office_categorisation.is_pep:
            office_occupancy = h.make_occupancy(
                context, person, office, categorisation=office_categorisation
            )
            if office_occupancy is not None:
                context.emit(office_occupancy)
                context.emit(office)
    context.emit(person)
```

## Multi-term source

The default shape whenever the source exposes past terms, not just the sitting roster.
See [Historical and multi-term sources](index.md#historical-and-multi-term-sources).

```python
from dataclasses import dataclass

TOPICS = ["gov.national", "gov.legislative"]


@dataclass(frozen=True)
class Term:
    """A parliamentary term. `period_end` is None for the sitting parliament."""

    id: str
    period_start: str
    period_end: str | None


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: Term,
    row: dict[str, Any],
) -> None:
    person = context.make("Person")
    person.id = context.make_id(row.pop("member_id"))
    person.add("name", row.pop("name"), lang="eng")
    person.add("citizenship", "xx")
    # ... remaining person props ...

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=term.period_start,
        period_end=term.period_end,
        no_end_implies_current=term.period_end is None,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", row.pop("district", None))
    occupancy.add("politicalGroup", row.pop("group", None))
    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=["photo_url"])


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Examplia",
        country="xx",
        topics=TOPICS,
        wikidata_id="Q...",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    cutoff = h.earliest_term_start(TOPICS)
    # Newest first, so the first out-of-window term ends the loop rather than
    # skipping one term and carrying on through the whole archive. Sort on the
    # start date, not on a term id — numeric ids held as strings sort "10" < "9".
    terms = sorted(discover_terms(context), key=lambda t: t.period_start, reverse=True)
    for term in terms:
        # ISO strings compare correctly here, including year-only term bounds.
        if term.period_end is not None and term.period_end < cutoff:
            context.log.info("Term predates the PEP window; skipping", term=term.id)
            break
        crawl_term(context, position, categorisation, term)
```

`discover_terms` reads the terms from the source (a term switcher in the HTML, an
`ElectionId` parameter, a `/legislaturas` endpoint); see
[Noticing a new term](index.md#noticing-a-new-term).
