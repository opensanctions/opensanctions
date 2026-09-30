# PEP Crawler Examples

## Reference crawler: a finished national legislature

This is what "done" looks like: a crawler for Luxembourg's Chamber of Deputies, written
to current practice. One position, a current-only roster published as a CSV, per-person
start dates. Match its shape before reaching for anything more elaborate; the other
patterns in this file are variations on it. It's based on `lu_chamber`, but work from
this version: the crawler in the repo predates these conventions.

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
            # Contact details are not extracted for PEPs.
            "address",
            "phone_ext",
            "phone_mobile",
            "email",
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

The YAML side — date formats and the gender values live in metadata, not in code:

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

What first drafts most often get wrong, and this gets right:

- **The row loop feeds `crawl_row` directly** — no discovery helper collecting IDs
  first, no repeated requests "to be safe", no hand-written row-count check (the YAML
  `assertions` own totals).
- **`categorise` runs once, in `crawl()`**, next to the position it gates.
- **Each workaround carries its reason** (the uncached catalog lookup, the `cp1252`
  encoding), so a reviewer can tell necessary code from machinery.
- **Status comes from the dates.** This roster is republished daily and drops departed
  members, so `make_occupancy`'s defaults are right. Where dates can't be trusted,
  see SKILL.md, Step 3.

## Pattern B: Mixed dataset with `default_is_pep=None` (declaration-style)

For sources that list officials across many roles where some are PEP and some aren't.
PEP status is determined by the UI review workflow, not the crawler.

```python
def crawl_member(context: Context, row: dict[str, Any]) -> None:
    role = row.pop("role")  # source-supplied, in French
    # No topics: the crawler doesn't know which position this is, so the review and
    # classification system decides both PEP status and topics.
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

Key differences from the reference crawler:
- `default_is_pep=None` — positions start uncategorised; the UI must mark them.
- `no_end_implies_current=False` — a declaration doesn't prove current office.
- `status=OccupancyStatus.UNKNOWN` — the declaration gives no dates to derive a status
  from, and with none `make_occupancy` would drop the person. This is the one case
  where overriding is right; if the source does give dates, pass them and drop the
  override, since an explicit status also skips the death and age checks.
- Position is created per-record (each unique role string becomes a position).

### Subnational variant (per-municipality / per-region positions)

Same `default_is_pep=None` shape as Pattern B, but used for sources where each record names
a sub-national position (e.g. mayor of municipality X). Two extras:

- Translate the role label to English via a `position` lookup. This is only
  applicable when the source has very few distinct position names (a handful of
  role labels shared across all municipalities). Otherwise, pass the
  source-language label with `lang=` and `translate_name=True` as in Pattern B.
- Pass `subnational_area=...` and **omit `wikidata_id`** — a Wikidata ID would
  collapse every municipality into the same entity.

```python
res = context.lookup("position", row.pop("MAN_LABEL"))
assert res is not None, f"Unknown position: {row['MAN_LABEL']!r}"
position = h.make_position(
    context,
    name=f"{res.value} of {commune_label}",   # English name + locality
    country="lu",
    topics=["gov.muni", "gov.executive"],     # the role and tier are known; only the locality varies
    subnational_area=commune_label,           # NOT wikidata_id — per-locality
    lang="eng",                               # already English after the lookup
)
categorisation = categorise(context, position, default_is_pep=None)
if not categorisation.is_pep:
    return  # one locality among many — skip it, as in Pattern B
```

YAML side — declare the translation lookup:

```yaml
lookups:
  position:
    options:
      - match: Bourgmestre
        value: Mayor
      - match: Échevin
        value: Alderman
```

## Pattern C: Several positions in one crawler

Where a position's definition belongs — a `position` lookup keyed on the source's own
label, or arguments to `h.make_position` in the crawler — is decided in `zavod/docs/peps.md`
→ "Where a position's definition belongs". Read that first; this is only the code shape
for the lookup case.

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
context.emit(position)
```

### One label, several held positions

When a role label implies more than one held office — a Speaker is elected from among
the members and keeps their seat — map the label to every held position name with a
multi-`values` lookup option, and make an occupancy for each
(`zavod/docs/peps.md` → "One person, several positions"):

```yaml
lookups:
  position:
    required: true
    options:
      - match: Member of Parliament
        value: Member of the Parliament of Examplia
      - match: Speaker
        values:
          - Speaker of the Parliament of Examplia
          - Member of the Parliament of Examplia
```

```python
# Only the member seat has a Wikidata item.
POSITION_QIDS = {"Member of the Parliament of Examplia": "Q..."}

# required: true only halts the crawl via context.lookup —
# context.lookup_value catches the exception and returns None.
res = context.lookup("position", role)
assert res is not None, role
for title in res.values:
    position = h.make_position(
        context,
        name=title,
        country="xx",
        topics=["gov.national", "gov.legislative"],
        wikidata_id=POSITION_QIDS.get(title),
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        continue
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is not None:
        context.emit(occupancy)
        # Repeated emits of the same position/person are fine.
        context.emit(position)
        context.emit(person)
```

## Multi-term source

The default shape whenever the source exposes past terms, not just the sitting roster.
Term bounds go in `period_start`/`period_end` (identical for everyone who served that
term); a personal entry or exit date, where the source gives one, goes in
`start_date`/`end_date` alongside them.

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
    person.id = context.make_id(row.pop("member_id"))  # a stable source id, not the name
    person.add("name", row.pop("name"), lang="eng")
    person.add("citizenship", "xx")  # cite the electoral law in a comment here
    # ... remaining person props, all set BEFORE make_occupancy ...

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=term.period_start,
        period_end=term.period_end,
        # Per occupancy, not per dataset: only the sitting term is still open.
        no_end_implies_current=term.period_end is None,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", row.pop("district", None))
    # The parliamentary faction, distinct from Person:political party membership.
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
`ElectionId` parameter, a `/legislaturas` endpoint). Where the source labels terms but
gives no dates, map the ordinal to its election year in a module-level table and
**warn on an ordinal missing from it**, so a newly added term surfaces as maintenance
work instead of being emitted with guessed dates.
