import re

import xlrd
from normality import latinize_text
from rigour.mime.types import XLS
from zavod.shed.trans import (
    ENGLISH,
    apply_translit_full_name,
    make_name_translit_prompt,
    run_translation_prompt,
)
from zavod.stateful.review import assert_all_accepted
from zavod.util import LangText

from zavod import Context, Entity
from zavod import helpers as h

# "<Arabic name>، بالعبرية: <Hebrew name>" / "…، بالإنجليزية: <English name>"
SCRIPT_MARKER = re.compile(r"\s*،?\s*بال(عبرية|إنجليزية)\s*:?\s*")
MARKER_LANGS = {"عبرية": "heb", "إنجليزية": "eng"}
# "…، الأصل/ الولايات المتحدة" - country of origin attached to parent names
ORIGIN_SUFFIX = re.compile(r"،\s*الا?[أا]صل\s*/.*$")
# "(حسب الجنسية الفرنسية)" - "(according to the French nationality)"
NATIONALITY_NOTE = re.compile(r"\(حسب [^)]*\)")
DOC_NUMBER = re.compile(r"^\d{1,9}$")


def split_scripts(text: str) -> list[tuple[str, str]]:
    """Split "<Arabic>، بالعبرية: <Hebrew>" into (value, lang) pairs."""
    parts = SCRIPT_MARKER.split(text)
    names = [(parts[0], "ara")]
    for marker, value in zip(parts[1::2], parts[2::2]):
        names.append((value, MARKER_LANGS[marker]))
    return [(v.strip(" ،:"), lang) for v, lang in names if v.strip(" ،:")]


def apply_names(context: Context, entity: Entity, raw_name: str) -> None:
    """Apply the Arabic and Hebrew/English parts of the name, sending irregular
    parts to human review, and add an LLM-cleaned English transliteration."""
    parts = split_scripts(raw_name)
    by_lang = {lang: value for value, lang in parts}
    assert len(by_lang) == len(parts), parts
    # Source names are legal designations: irregular ones are reviewed by a human.
    for value, lang in parts:
        original = h.Names()
        original.add("name", value)
        h.apply_reviewed_names(context, entity, original=original, lang=lang)
    if "eng" in by_lang:
        return
    # The Arabic is itself a rendering of the Hebrew, so prefer the Hebrew form.
    lang = "heb" if "heb" in by_lang else "ara"
    prompt = make_name_translit_prompt(lang, [ENGLISH])
    result = run_translation_prompt(context, prompt=prompt, text=by_lang[lang])
    translit = result.get_preferred_language()
    if translit is None:
        return
    # The transliteration is ours, not the source's, so notes carried over from
    # the source (e.g. a nickname) are left to LLM cleaning.
    original = h.Names()
    original.add("name", translit.text)
    h.apply_reviewed_names(
        context,
        entity,
        original=original,
        lang="eng",
        llm_cleaning=True,
        default_accepted=True,
    )


def parse_parent_names(raw: str | None) -> list[str]:
    """Parent name cells mix Arabic and Hebrew forms, e.g. "عامي بوبر (עמי פופר)"."""
    if raw is None:
        return []
    raw = ORIGIN_SUFFIX.sub("", raw)
    raw = NATIONALITY_NOTE.sub("", raw)
    parts = h.multi_split(raw, ["بالعبرية", "(", ")", "/"])
    return [p.strip(" ،:") for p in parts if p.strip(" ،:")]


def crawl_person(context: Context, row: dict[str, str | None]) -> None:
    raw_name = row.pop("name")
    assert raw_name is not None, row
    raw_doc_number = row.pop("doc_number")
    assert raw_doc_number is not None, row

    # Excel stripped the leading zeros off the 9-digit ID numbers.
    # Check if this is still corrupted and correct with zfill()
    assert DOC_NUMBER.match(raw_doc_number), raw_doc_number
    doc_number = raw_doc_number.zfill(9)

    person = context.make("Person")
    person.id = context.make_id(raw_doc_number)
    apply_names(context, person, raw_name)
    for alias, lang in split_scripts(row.pop("alias") or ""):
        person.add("alias", alias, lang=lang)
        if alias != latinize_text(alias):
            apply_translit_full_name(context, person, LangText(alias, lang), alias=True)
    person.add("gender", row.pop("gender"))
    person.add("fatherName", parse_parent_names(row.pop("father_name")))
    person.add("motherName", parse_parent_names(row.pop("mother_name")))
    h.apply_date(person, "birthDate", row.pop("birth_date"))
    person.add("nationality", row.pop("nationality"))

    addr = h.make_address(context, full=row.pop("address"), lang="ara")
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
            # Lookup values are the entity names as listed, from which their IDs derive.
            org_id = context.make_id(res.value)
            link = context.make("UnknownLink")
            link.id = context.make_id(person.id, "affiliation", org_id)
            link.add("subject", person)
            link.add("object", org_id)
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


def crawl_organization(context: Context, row: dict[str, str | None]) -> None:
    raw_name = row.pop("entity_name")
    assert raw_name is not None, row

    org = context.make("Organization")
    org.id = context.make_id(raw_name)
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

    # Entities
    for row in h.parse_xls_sheet(context, book["الكيانات"], skiprows=2):
        row = {context.lookup_value("columns", k, k) or k: v for k, v in row.items()}
        crawl_organization(context, row)

    # Individuals
    for row in h.parse_xls_sheet(context, book["الأفراد"], skiprows=2):
        row = {context.lookup_value("columns", k, k) or k: v for k, v in row.items()}
        crawl_person(context, row)

    assert_all_accepted(context, raise_on_unaccepted=False)
