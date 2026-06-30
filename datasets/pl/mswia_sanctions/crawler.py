from followthemoney.types import registry
from pydantic import BaseModel

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract.llm import run_typed_text_prompt, DEFAULT_MODEL
from zavod.stateful.review import (
    JSONSourceValue,
    assert_all_accepted,
    review_extraction,
)

EXTRACT_PROMPT = """Extract structured data from an identification details string taken from a Polish sanctions list entry.

The input is a Polish-language text that may contain various combinations of:
- NIP (Polish tax ID), INN (Russian tax ID / ИНН), TIN, VAT, and other tax identifiers
- REGON (Polish statistical number), KRS (Polish business register), BIN (Kazakh business ID),
  and other business/company registration numbers
- PESEL (Polish personal ID) and other national personal ID numbers
- A registered address (often preceded by "siedziba:", "adres:", or similar)
- Russian/Arabic/other-script addresses alongside a Polish transliteration — include both as
  separate address entries
- Date of birth (e.g. "urodzon* DD miesiąc YYYY r."), using Polish month names
- Place of birth (e.g. "w Moskwie", "w Leningradzie", "w Wilnie")
- Citizenship (e.g. "obywatel Federacji Rosyjskiej", "obywatelka Republiki Białorusi")
- Company codes for non-Polish registries ("kod przedsiębiorstwa", "numer rejestrowy",
  "identyfikator rejestracyjny", "numer identyfikacyjny BIN")
- OGRN / ОГРН (Russian primary state registration number)
- KPP / КПП (Russian tax registration reason code)
- OKPO / ОКПО (Russian classifier of enterprises and organizations)
- DUNS numbers

Extract the following fields:
  taxNumber          - NIP, TIN, VAT, and other tax identifiers (not INN)
  innCode            - Russian INN (ИНН / numer INN) specifically
  registrationNumber - REGON, KRS, BIN, and other business/company registration numbers
  idNumber           - PESEL and other personal ID numbers
  ogrnCode           - Russian OGRN (ОГРН) specifically
  kppCode            - Russian KPP (КПП) specifically
  dunsCode           - DUNS number specifically
  okpoCode           - Russian OKPO (ОКПО) specifically
  address            - all registered addresses; include both transliterated and original-script
                       versions as separate entries; preserve text exactly
  birthDate          - date of birth in ISO 8601 (YYYY-MM-DD, YYYY-MM, or YYYY)
  birthPlace         - city/place of birth (English name if well-known, otherwise as given)
  citizenship        - ISO 2-letter country code(s) (e.g. RU, BY, PL, UA)

Rules:
- Preserve address text exactly — no translation.
- Convert Polish month names to ISO date format for birthDate.
- For every code/number field (taxNumber, innCode, registrationNumber, idNumber, ogrnCode,
  kppCode, dunsCode, okpoCode), return ONLY the bare number. Strip any leading label, prefix,
  or qualifier such as "OKPO", "REGON", "NIP", "INN", "OGRN", "KPP", "DUNS", "ИНН", "ОГРН",
  "КПП", "ОКПО", ":", etc. For example "OKPO 12345678" must be returned as "12345678".
- Return empty lists for fields with no value.
"""

# "osoby" = persons, "podmioty" = entities
TYPES = {"osoby": "Person", "podmioty": "Company"}


class DetailsData(BaseModel):
    taxNumber: list[str] = []
    innCode: list[str] = []
    registrationNumber: list[str] = []
    idNumber: list[str] = []
    ogrnCode: list[str] = []
    kppCode: list[str] = []
    dunsCode: list[str] = []
    okpoCode: list[str] = []
    address: list[str] = []
    birthDate: list[str] = []
    birthPlace: list[str] = []
    citizenship: list[str] = []


class DetailsExtractionResult(BaseModel):
    details: DetailsData


def extract_details(context: Context, entity: Entity, details: str) -> None:
    source_value = JSONSourceValue(
        key_parts=[details],
        label="details extraction",
        data=details,
    )
    result = run_typed_text_prompt(
        context=context,
        prompt=EXTRACT_PROMPT,
        string=details,
        response_type=DetailsExtractionResult,
        model=DEFAULT_MODEL,
    )
    review = review_extraction(
        context=context,
        source_value=source_value,
        original_extraction=result,
        origin=DEFAULT_MODEL,
    )
    if not review.accepted:
        return
    data = review.extracted_data.details
    # Each DetailsData field name maps 1:1 to an FTM property; apply every value.
    for prop, values in data:
        if prop == "birthDate":
            for value in values:
                h.apply_date(entity, prop, value)
        else:
            entity.add(prop, values)


def crawl_row(context: Context, row: dict[str, str | None], table_title: str) -> None:
    # "data_umieszczenia_na_liscie" = date of placement on the list
    listing_date = row.pop("data_umieszczenia_na_liscie")
    if listing_date is None:
        context.log.warn("No listing date", row=row)
        return

    entity = context.make(TYPES[table_title])
    # "nazwisko_i_imie" = surname and first name; "nazwa_podmiotu" = entity name
    name_raw = row.pop("nazwisko_i_imie", None) or row.pop("nazwa_podmiotu", None)
    if name_raw is None:
        context.log.warn("No name", row=row)
        return

    entity.id = context.make_slug(table_title, name_raw)
    # Normal case: LASTNAME Firstname (Alias) / Company Name (Alias)
    # "w zapisie także" = also written as; "lub" = or
    names = h.multi_split(name_raw, ["(w zapisie także", "(", ")", "lub", ","])

    if entity.schema.name == "Person":
        # For Persons, we apply all available names as name (not alias), because they are
        # usually just different scripts or spelling, not different names.
        for name in names:
            name = name.strip("„”")
            name_parts = name.split(" ")

            assert len(name_parts) > 1, len(name_parts)
            if len(name_parts) == 2:
                last_name, first_name = name_parts
                h.apply_name(
                    entity,
                    first_name=first_name,
                    last_name=last_name,
                )
            else:
                res = context.lookup("names", name)
                if res is None:
                    context.log.warning("Unhandled person name", name=name)
                    h.apply_name(entity, full=name)
                else:
                    h.apply_name(
                        entity,
                        first_name=res.first_name,
                        last_name=res.last_name,
                        patronymic=res.patronymic,
                    )

    else:
        # "w likwidacji" = in liquidation; "w upadłości" = in bankruptcy
        name = names[0].removesuffix(" w likwidacji").removesuffix(" w upadłości")
        entity.add("name", name)

        alias = names[1] if len(names) > 1 else ""
        # "nazwa rosyjskojęzyczna" = russian name / "rosyjskim" = Russian
        if alias.startswith("nazwa rosyjskojęzyczna: ") or "rosyjskim: " in alias:
            entity.add("name", alias.split(": ", 1)[1], lang="ru")
        # "nazwa arabska" = arabic name
        elif alias.startswith("nazwa arabska: "):
            entity.add("name", alias.removeprefix("nazwa arabska: "), lang="ara")
        # "poprzednio" = previously
        elif alias.startswith("poprzednio: "):
            entity.add("previousName", alias.removeprefix("poprzednio: "))
        else:
            # "obecnie" = currently
            # "inaczej" = otherwise
            # "lub" = or
            aliases = h.multi_split(alias, ["lub", "obecnie:", "inaczej:"])
            # Aliases are often in quotes
            cleaned_aliases = [
                a.removesuffix(" w likwidacji")
                .removesuffix(" w upadłości")
                .replace("„", "")
                .replace("”", "")
                for a in aliases
            ]
            for uncleaned_alias, cleaned_alias in zip(aliases, cleaned_aliases):
                entity.add("alias", cleaned_alias, original_value=uncleaned_alias)

    # "uzasadnienie_wpisu_na_liste" = justification for placement on the list
    notes = row.pop("uzasadnienie_wpisu_na_liste")
    entity.add("notes", notes)

    # "dane_identyfikacyjne_podmiotu/osoby" = identification data of entity/person
    details = row.pop("dane_identyfikacyjne_podmiotu", None)
    details = row.pop("dane_identyfikacyjne_osoby", details)
    if details is not None and details.strip():
        extract_details(context, entity, details)

    sanction = h.make_sanction(context, entity)
    # "zastosowane_srodki_sankcyjne" = applied sanctions measures
    provisions = row.pop("zastosowane_srodki_sankcyjne")
    assert provisions is not None
    if len(provisions) > registry.string.max_length:
        sanction.add("description", provisions)
        sanction.add("provisions", "See description.")
    else:
        sanction.add("provisions", provisions)

    h.apply_date(sanction, "startDate", listing_date)
    # "data_wykreslenia_z_listy" = date of removal from the list
    end_date = row.pop("data_wykreslenia_z_listy", None)
    h.apply_date(sanction, "endDate", end_date)
    if not end_date:
        entity.add("topics", "sanction")
    context.audit_data(row)
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    # "Osoby" = Persons
    table = h.xpath_element(
        doc, ".//h3[text() = 'Osoby']/following-sibling::div[1]//table"
    )
    for row in h.parse_html_table(table, header_tag="td"):
        crawl_row(context, h.cells_to_str(row), "osoby")

    # "Podmioty" = Entities
    # Pretty special xpath because they have some <table><tr><table> thing going on
    table = h.xpath_element(
        doc, ".//h3[text() = 'Podmioty']/following-sibling::div[1]//table//tr//table"
    )
    for row in h.parse_html_table(table, header_tag="td"):
        crawl_row(context, h.cells_to_str(row), "podmioty")

    assert_all_accepted(context, raise_on_unaccepted=False)
