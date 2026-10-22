import re

import xlrd
from rigour.mime.types import XLS
from rigour.names.org_types import extract_org_types
from rigour.text.scripts import text_scripts
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.review import assert_all_accepted
from zavod.util import LangText

from zavod import Context, Entity
from zavod import helpers as h

# "<Arabic name>، بالعبرية: <Hebrew name>" / "…، بالإنجليزية: <English name>"
SCRIPT_LANGS = {"بالعبرية": "heb", "بالإنجليزية": "eng"}
SCRIPT_MARKER = re.compile(rf"\s*،?\s*({'|'.join(SCRIPT_LANGS)})\s*:?\s*")
# "…، الأصل/ الولايات المتحدة" - country of origin attached to parent names
ORIGIN_SUFFIX = re.compile(r"،\s*(الأصل|الاصل)\s*/.*$")
# Generic rigour org types that are company legal forms, e.g. المحدودة / בע"מ (Ltd)
COMPANY_TYPES = {"llc", "jsc", "llp", "lp"}


def split_scripts(text: str) -> list[tuple[str, str]]:
    """Split "<Arabic>، بالعبرية: <Hebrew>" into (value, lang) pairs."""
    parts = SCRIPT_MARKER.split(text)
    names = [(parts[0], "ara")]
    for marker, value in zip(parts[1::2], parts[2::2]):
        names.append((value, SCRIPT_LANGS[marker]))
    return [(v.strip(), lang) for v, lang in names if v.strip()]


def apply_names(
    context: Context, entity: Entity, raw_name: str, prop: str = "name"
) -> None:
    """Apply the Arabic and Hebrew/English parts of the name, sending irregular
    parts to human review, and add an English transliteration."""
    parts = split_scripts(raw_name)
    by_lang = {lang: value for value, lang in parts}
    assert len(by_lang) == len(parts), parts
    # Source names are legal designations: irregular ones are reviewed by a human.
    for value, lang in parts:
        original = h.Names()
        original.add(prop, value)
        h.apply_reviewed_names(context, entity, original=original, lang=lang)
    if "eng" in by_lang:
        return
    # The Arabic is itself a rendering of the Hebrew, so prefer the Hebrew form.
    lang = "heb" if "heb" in by_lang else "ara"
    # The transliteration keeps its origin, so it isn't mistaken for a source name.
    apply_translit_full_name(
        context, entity, LangText(by_lang[lang], lang), alias=prop == "alias"
    )


def apply_parent_names(entity: Entity, prop: str, raw: str | None) -> None:
    """Parent name cells mix Arabic and Hebrew forms, e.g. "عامي بوبر (עמי פופר)"."""
    if raw is None:
        return
    raw = ORIGIN_SUFFIX.sub("", raw)
    for part in h.multi_split(raw, [*SCRIPT_LANGS, "(", ")", "/"]):
        part = part.strip(" ،:")
        if part:
            lang = "heb" if "Hebrew" in text_scripts(part) else "ara"
            entity.add(prop, part, lang=lang)


def crawl_person(
    context: Context, row: dict[str, str | None], orgs: dict[str, Entity]
) -> None:
    raw_name = row.pop("name")
    assert raw_name is not None, row
    raw_doc_number = row.pop("doc_number")
    assert raw_doc_number is not None, row

    # Excel stripped the leading zeros off the 9-digit ID numbers.
    # Check if this is still corrupted and correct with zfill()
    assert re.fullmatch(r"\d{1,9}", raw_doc_number), raw_doc_number
    doc_number = raw_doc_number.zfill(9)

    person = context.make("Person")
    person.id = context.make_id(raw_doc_number)
    apply_names(context, person, raw_name)
    alias = row.pop("alias")
    if alias is not None and alias != "-":
        apply_names(context, person, alias, prop="alias")
    person.add("gender", row.pop("gender"))
    apply_parent_names(person, "fatherName", row.pop("father_name"))
    apply_parent_names(person, "motherName", row.pop("mother_name"))
    h.apply_date(person, "birthDate", row.pop("birth_date"))
    person.add("nationality", row.pop("nationality"))

    raw_address = row.pop("address")
    assert raw_address is not None, row
    res = context.lookup("address", raw_address)
    # "وفقا لبيانات اخرى" ("according to other data") introduces an alternative
    # address, "الكيان المرتبط" ("affiliated entity") a misplaced affiliation.
    if res is None and ("لبيانات" in raw_address or "الكيان المرتبط" in raw_address):
        context.log.warning("Address needs a lookup", address=raw_address)
    for address in [raw_address] if res is None else res.values:
        addr = h.make_address(context, full=address, lang="ara")
        h.copy_address(person, addr)

    doc_type = row.pop("doc_type")
    res = (
        None
        if doc_type is None
        else context.lookup("doc_type", doc_type, warn_unmatched=True)
    )
    if res is None:
        person.add("idNumber", doc_number)
    else:
        ident = h.make_identification(
            context,
            person,
            number=doc_number,
            doc_type=res.doc_type,
            country=res.country,
        )
        if ident is not None:
            context.emit(ident)

    affiliation = row.pop("affiliation")
    if affiliation is not None:
        res = context.lookup("affiliation", affiliation, warn_unmatched=True)
        if res is not None and res.value is not None:
            # Lookup values are the entity names as listed in the entities sheet.
            org = orgs[res.value]
            link = context.make("UnknownLink")
            link.id = context.make_id(person.id, "affiliation", org.id)
            link.add("subject", person)
            link.add("object", org)
            link.add("role", "Affiliated entity")
            context.emit(link)

    sanction = h.make_sanction(context, person, program_key="PS-UNSC1373")
    h.apply_date(sanction, "listingDate", row.pop("listing_date"))
    sanction.add("summary", row.pop("amendment"), lang="ara")
    h.apply_date(sanction, "modifiedAt", row.pop("amendment_date"))
    person.add("topics", "sanction")
    context.emit(person)
    context.emit(sanction)
    # The running number is not stable across list versions.
    context.audit_data(row, ignore=["number"])


def crawl_organization(
    context: Context, row: dict[str, str | None], orgs: dict[str, Entity]
) -> None:
    raw_name = row.pop("entity_name")
    assert raw_name is not None, row

    org_types = {t for _, t in extract_org_types(raw_name.casefold(), generic=True)}
    org = context.make("Company" if org_types & COMPANY_TYPES else "Organization")
    org.id = context.make_id(raw_name)
    orgs[raw_name] = org
    apply_names(context, org, raw_name)

    sanction = h.make_sanction(context, org, program_key="PS-UNSC1373")
    h.apply_date(sanction, "listingDate", row.pop("listing_date"))
    h.apply_date(sanction, "modifiedAt", row.pop("amendment_date"))
    sanction.add("summary", row.pop("amendment"), lang="ara")
    sanction.add("summary", row.pop("description"), lang="ara")
    org.add("topics", "sanction")
    context.emit(org)
    context.emit(sanction)
    context.audit_data(row)


def crawl(context: Context) -> None:
    # The national list link is an ASP.NET postback, which redirects to the file.
    # no cache so we fetch the fresh view state that carries the download link
    # for ASP.NET to rebuild from
    doc = context.fetch_html(context.data_url)
    # build the POST data from the hidden fields in the form
    form = h.xpath_element(doc, "//form[@id='form1']")
    data = {
        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$BulletedList2",
        "__EVENTARGUMENT": "0",
    }
    for field in h.xpath_elements(form, ".//input[@type='hidden']"):
        name = field.get("name")
        assert name is not None, field
        data[name] = field.get("value", "")
    path = context.fetch_resource(
        "source.xls", context.data_url, method="POST", data=data
    )
    context.export_resource(path, XLS, title=context.SOURCE_TITLE)
    book = xlrd.open_workbook(path.as_posix(), encoding_override="cp1256")
    assert book.sheet_names() == ["الأفراد", "الكيانات"], book.sheet_names()

    # Entities, keyed by their listed name for the individuals' affiliations
    orgs: dict[str, Entity] = {}
    for row in h.parse_xls_sheet(context, book["الكيانات"], skiprows=2):
        row = {context.lookup_value("columns", k, k) or k: v for k, v in row.items()}
        crawl_organization(context, row, orgs)

    # Individuals
    for row in h.parse_xls_sheet(context, book["الأفراد"], skiprows=2):
        row = {context.lookup_value("columns", k, k) or k: v for k, v in row.items()}
        crawl_person(context, row, orgs)

    assert_all_accepted(context, raise_on_unaccepted=False)
