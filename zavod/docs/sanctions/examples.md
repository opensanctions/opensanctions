# Sanctions crawler examples

Short synthetic crawlers that show the rules in [developing sanctions crawlers](index.md)
working together.

## Consolidated list with programs

A JSON API listing each designation with labelled names and one or more regimes.

```yaml
names:
  schema_rules:
    LegalEntity:
      # Set from studying the source's names.
      reject_chars: "()"

lookups:
  type:
    options:
      - match: Individual
        value: Person
      - match: Entity
        value: LegalEntity
      - match: Ship
        value: Vessel
  name_type:
    options:
      - match: Primary name
        value: name
      - match: AKA
        value: alias
      - match: Low quality AKA
        value: weakAlias
      - match: FKA
        value: previousName
  sanction.program:
    options:
      - match: The Russia (Sanctions) (EU Exit) Regulations 2019
        value: GB-RUS
      - match: The Cyber (Sanctions) (EU Exit) Regulations 2020
        value: GB-CYB
  sanction.measure:
    options:
      - match: Asset freeze
        value: Asset freeze
      - match: Travel ban
        value: Travel ban
      - match: Trust services sanctions
        value: Services ban
```

```python
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.stateful.review import assert_all_accepted


def crawl_designation(context: Context, item: dict[str, Any]) -> None:
    schema = context.lookup_value("type", item.pop("type"), warn_unmatched=True)
    if schema is None:
        return
    entity = context.make(schema)
    entity.id = context.make_slug(item.pop("id"))

    names = h.Names()
    for name in item.pop("names"):
        prop = context.lookup_value("name_type", name["type"], warn_unmatched=True)
        if prop is None:
            return
        names.add(prop, name["value"])
    h.apply_reviewed_names(context, entity, original=names)

    dob = item.pop("date_of_birth")
    imo_number = item.pop("imo_number")
    flag = item.pop("flag")
    if entity.schema.is_a("Person"):
        h.apply_date(entity, "birthDate", dob)
    if entity.schema.is_a("Vessel"):
        entity.add("imoNumber", imo_number)
        entity.add("flag", flag)

    for regime in item.pop("regimes"):
        program = regime.pop("name")
        sanction = h.make_sanction(
            context,
            entity,
            key=program,
            program_name=program,
            source_program_key=program,
            program_key=h.lookup_sanction_program_key(context, program),
        )
        h.apply_date(sanction, "listingDate", regime.pop("date_published"))
        h.apply_date(sanction, "startDate", regime.pop("date_in_force"))
        for measure in regime.pop("measures"):
            sanction.add(
                "provisions",
                context.lookup_value("sanction.measure", measure, warn_unmatched=True),
            )
        context.audit_data(regime)
        context.emit(sanction)

    entity.add("topics", "sanction")
    context.emit(entity)
    context.audit_data(item)


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url)
    for item in data["designations"]:
        crawl_designation(context, item)
    assert_all_accepted(context, raise_on_unaccepted=False)
```

## Single national list

A spreadsheet with no type column, no reliable ID, and de-listed rows still present.
The list is one piece, so it sets no program.

```python
from python_calamine import CalamineWorkbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.stateful.review import assert_all_accepted


def crawl_row(context: Context, row: dict[str, str | None]) -> None:
    if row.pop("statut") == "Radié":
        context.log.info("Skipping de-listed entry", row=row)
        return
    name = row.pop("nom")
    dob = row.pop("date_de_naissance")
    nationality = row.pop("nationalite")

    entity = context.make("Person" if dob is not None else "LegalEntity")
    entity.id = context.make_id(name, dob, nationality)
    h.apply_reviewed_names(context, entity, original=h.Names(name=name), lang="fra")
    if entity.schema.is_a("Person"):
        h.apply_date(entity, "birthDate", dob)
        entity.add("nationality", nationality)
    else:
        entity.add("country", nationality)

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", row.pop("motif"), lang="fra")
    h.apply_date(sanction, "listingDate", row.pop("date_inscription"))

    entity.add("topics", "sanction")
    context.emit(sanction)
    context.emit(entity)
    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook = CalamineWorkbook.from_path(path)
    for row in h.parse_xlsx_sheet(context, workbook.get_sheet_by_index(0)):
        crawl_row(context, row)
    assert_all_accepted(context, raise_on_unaccepted=False)
```

## Linked parties

Owners, operators and wallets named alongside a designated entity.

```python
from zavod import Context, Entity
from zavod import helpers as h


def crawl_linked(context: Context, entity: Entity, row: dict[str, str | None]) -> None:
    owner_name = row.pop("parent_company")
    if owner_name is not None:
        owner = context.make("Organization")
        owner.id = context.make_id("owner", owner_name)
        owner.add("name", owner_name)
        context.emit(owner)

        entity.add_schema("Company")
        ownership = context.make("Ownership")
        ownership.id = context.make_id(owner.id, "owns", entity.id)
        ownership.add("owner", owner)
        ownership.add("asset", entity)
        context.emit(ownership)

    operator_name = row.pop("previous_operator")
    if operator_name is not None:
        operator = context.make("Organization")
        operator.id = context.make_id("operator", operator_name)
        operator.add("name", operator_name)
        context.emit(operator)

        link = context.make("UnknownLink")
        link.id = context.make_id(operator.id, "operated", entity.id)
        link.add("subject", operator)
        link.add("object", entity)
        link.add("role", "Previous operator")
        context.emit(link)

    for address, currency in h.extract_cryptos(row.pop("other_information")).items():
        wallet = context.make("CryptoWallet")
        wallet.id = context.make_id(address)
        wallet.add("publicKey", address)
        wallet.add("currency", currency)
        wallet.add("holder", entity)
        context.emit(wallet)
```
