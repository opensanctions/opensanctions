import re

from python_calamine import CalamineWorkbook
from rigour.mime.types import XLS
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
# Address cells that name more than one place or carry other notes
ADDRESS_MARKERS = ["لبيانات", "يتواجد", "يقيم", "يسكن", "حالياً", "الكيان المرتبط"]


def split_scripts(text: str) -> dict[str, str]:
    """Split "<Arabic>، بالعبرية: <Hebrew>" into its forms, keyed by language."""
    parts = SCRIPT_MARKER.split(text)
    assert text_scripts(parts[0]) == {"Arabic"}, text
    names = {"ara": parts[0].strip()}
    for marker, value in zip(parts[1::2], parts[2::2]):
        lang = SCRIPT_LANGS[marker]
        assert lang not in names, text
        names[lang] = value.strip()
    return names


def apply_names(
    context: Context, entity: Entity, raw_name: str, prop: str = "name"
) -> None:
    """Apply the Arabic and Hebrew/English forms of the name, sending irregular
    ones to human review, and add English transliterations."""
    names = split_scripts(raw_name)
    for lang, value in names.items():
        h.apply_reviewed_name_string(
            context, entity, string=value, original_prop=prop, lang=lang
        )
    if "eng" in names:
        return
    # The Arabic is itself a rendering of the Hebrew, so prefer the Hebrew form.
    lang = "heb" if "heb" in names else "ara"
    # Transliterate the reviewed names; ones awaiting review are still irregular.
    for name_prop in ("name", "alias"):
        for stmt in entity.get_statements(name_prop):
            if stmt.lang == lang and not h.is_name_irregular(entity, stmt.value):
                apply_translit_full_name(
                    context,
                    entity,
                    LangText(stmt.value, lang),
                    alias=name_prop == "alias",
                )


def apply_parent_names(entity: Entity, prop: str, raw: str | None) -> None:
    """Parent name cells mix Arabic and Hebrew forms, e.g. "عامي بوبر (עמי פופר)"."""
    if raw is None:
        return
    raw = ORIGIN_SUFFIX.sub("", raw)
    for part in h.multi_split(raw, [*SCRIPT_LANGS, "(", ")", "/"]):
        part = part.strip(" ،:")
        entity.add(prop, part, lang="heb" if "Hebrew" in text_scripts(part) else "ara")


def crawl_person(
    context: Context, row: dict[str, str | None], orgs: dict[str, Entity]
) -> None:
    raw_name = row.pop("name")
    assert raw_name is not None, row
    raw_doc_number = row.pop("doc_number")
    assert raw_doc_number is not None, row

    # Excel strips the leading zeros off the 9-digit ID numbers; restore them.
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
    address_res = context.lookup("address", raw_address)
    for address in [raw_address] if address_res is None else address_res.values:
        # "وفقا لبيانات اخرى" ("according to other data") introduces an alternative
        # address, "يتواجد" ("stays at"), "يقيم"/"يسكن" ("lives") and "حالياً"
        # ("currently") a second place of residence, "الكيان المرتبط" ("affiliated
        # entity") a misplaced affiliation. Hebrew is a duplicate place name.
        if address is not None and (
            any(m in address for m in ADDRESS_MARKERS)
            or "Hebrew" in text_scripts(address)
        ):
            context.log.warning("Address needs a lookup", address=address)
        h.copy_address(person, h.make_address(context, full=address, lang="ara"))

    doc_type = row.pop("doc_type")
    doc_res = (
        None
        if doc_type is None
        else context.lookup("doc_type", doc_type, warn_unmatched=True)
    )
    if doc_res is None:
        person.add("idNumber", doc_number)
    else:
        ident = h.make_identification(
            context,
            person,
            number=doc_number,
            doc_type=doc_res.doc_type,
            country=doc_res.country,
        )
        if ident is not None:
            context.emit(ident)

    affiliation = row.pop("affiliation")
    if affiliation is not None:
        org_res = context.lookup("affiliation", affiliation, warn_unmatched=True)
        if org_res is not None and org_res.value is not None:
            # Lookup values are the entity names as listed in the entities sheet.
            org = orgs[org_res.value]
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

    # Mostly settler groups, outposts and movements rather than companies.
    org = context.make("Organization")
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
    # The list link is an ASP.NET postback: fetch the page uncached for a fresh
    # view state, then post its hidden form fields back to get the file.
    doc = context.fetch_html(context.data_url)
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
    workbook = CalamineWorkbook.from_path(path)
    assert workbook.sheet_names == ["الأفراد", "الكيانات"], workbook.sheet_names

    # Entities, keyed by their listed name for the individuals' affiliations
    orgs: dict[str, Entity] = {}
    for row in h.parse_xlsx_sheet(
        context, workbook.get_sheet_by_name("الكيانات"), skiprows=2
    ):
        row = {context.lookup_value("columns", k, k) or k: v for k, v in row.items()}
        crawl_organization(context, row, orgs)

    # Individuals
    for row in h.parse_xlsx_sheet(
        context, workbook.get_sheet_by_name("الأفراد"), skiprows=2
    ):
        row = {context.lookup_value("columns", k, k) or k: v for k, v in row.items()}
        crawl_person(context, row, orgs)

    assert_all_accepted(context, raise_on_unaccepted=False)
