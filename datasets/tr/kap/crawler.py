import json
import re
from typing import Any

from followthemoney import registry
from normality import normalize
from rigour.ids import LEI

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api

REGEX_ROW_ID = re.compile(rb"[0-9a-f]*:")
REGEX_ROW_REFERENCE = re.compile(r"^\$([0-9a-f]+)$")
# Subsidiary names are sometimes numbered by the filer: "7) Agrotech USA LLC".
REGEX_NUMBERED = re.compile(r"^\d+\)\s*")
# 11-digit values in a tax number field are national identity numbers (TCKN)
# of natural persons, not the 10-digit Turkish tax number (VKN).
REGEX_TCKN = re.compile(r"^\d{11}$")

BOARD_IGNORE = [
    # National identity or tax numbers of the member and of the natural person
    # acting for a legal person member, and their types. Never emitted.
    "tcknYknVkn",
    "credentialKey",
    "personWhoBehavesOnBehalfOfLegalPersonBoardMemberTcknVknYkn",
    "credentialKey2",
    "positionsHeldInTheCompanyInTheLastFiveYears",
    "currentPositionsHeldOutsideTheCompany",
    "fiveYearsExperience",
    "shareInCapital",
    "theShareGroupThatTheBoardMemberRepresenting",
    "linkToTheIndependencyDeclaration",
    "consideredByTheNominationCommittee",
    "satisfyTheIndependenceOrNot",
    "committeesChargedAndTask",
    "hideDelete",
]
ITEMS_IGNORE = [
    # Identifiers, capital and auditor, also given by the company list.
    "kpy41_acc4_vergi_no",
    "kpy41_acc4_vergi_dairesi",
    "kpy41_acc4_ticaret_sicil_numarasi",
    "kpy41_acc4_ticaret_sicil_memurlugu",
    "kpy41_acc4_tescil_tarihi",
    "kpy41_acc5_odenmis_sermaye",
    "kpy41_acc5_odenmis_sermaye_2",
    "kpy41_acc5_kayitli_sermaye_tavani",
    "kpy41_acc5_kayitli_sermaye_tavani_2",
    "kpy41_acc2_bdk",
    # Phone numbers, branch offices, production sites and contact persons.
    "kpy41_acc1_ilet_adres_tel_fax",
    "kpy41_acc1_merkez_disi_orgutleri",
    "kpy41_acc1_merkez_disi_orgutler_grid",
    "kpy41_acc1_uretim_adres",
    "kpy41_acc1_yatirimci_iliskileri",
    # Activity description, duration, licences and services (free text).
    "kpy41_acc2_faaliyet_konu",
    "kpy41_acc2_sure",
    "kpy41_acc2_yetki_belgeleri",
    "kpy41_acc2_sunulan_yan_hizmetler",
    "kpy41_acc2_borsa_uyelik_tarih",
    "kpy41_acc2_pazar_piyasa",
    "kpy41_acc8_spk_liste_giris",
    "kpy41_acc8_spk_liste_cikis",
    "kpy41_acc8_yabanci_lisans_anlasmasi",
    "kpy41_acc10_diger_hususlar",
    # Listing details, share classes and debt instruments.
    "kpy41_acc3_endeksler",
    "kpy41_acc3_sermaye_arac_pazar",
    "kpy41_acc3_son_durum_borsa_piyasalar",
    "kpy41_acc3_ortaklik_hakki_vermeyen",
    "kpy41_acc5_fiili_dolasimdaki_pay",
    "kpy41_acc5_sermayeyi_temsil_eden",
    "kpy41_acc5_sermayeyi_temsil_eden_2",
    # Senior managers, portfolio managers, other staff, partners of audit firms
    # and the companies an audit firm audits.
    "kpy41_acc6_yonetimde_soz_sahibi",
    "kpy41_acc6_portfoy_yoneticileri",
    "kpy41_acc6_sermaye_piyasa_faaliyetleri",
    "kpy41_acc6_sirketin_diger_personel",
    "kpy41_acc6_birimler_hizmetler",
    "kpy41_acc6_sorumlu_ortaklar",
    "kpy41_acc9_son_durum_denetledigi_kap",
]


def parse_rsc_rows(stream: bytes) -> dict[str, Any]:
    """Split a React Server Components stream into its rows, keyed by row ID.

    Each row is "<hex id>:<payload>". A payload is a single line of JSON, except
    for text rows, "T<hex byte length>,<text>", whose text can span lines. Module
    ("I") and resource hint ("HL") rows carry no data and are skipped.
    """
    rows: dict[str, Any] = {}
    pos = 0
    while pos < len(stream):
        match = REGEX_ROW_ID.match(stream, pos)
        if match is None:
            raise ValueError(
                f"Invalid RSC row at byte {pos}: {stream[pos : pos + 50]!r}"
            )
        row_id = match.group()[:-1].decode("ascii")
        pos = match.end()
        if stream.startswith(b"T", pos):
            comma = stream.index(b",", pos)
            end = comma + 1 + int(stream[pos + 1 : comma], 16)
            rows[row_id] = stream[comma + 1 : end].decode("utf-8")
            pos = end
            continue
        end = stream.find(b"\n", pos)
        if end < 0:
            end = len(stream)
        payload = stream[pos:end]
        pos = end + 1
        if payload.startswith((b"I[", b"HL[")):
            continue
        rows[row_id] = json.loads(payload)
    return rows


def resolve_references(value: Any, rows: dict[str, Any]) -> Any:
    """Replace RSC row references ("$4a") in a parsed value with the rows they
    point to. Strings that start with "$" are escaped as "$$"."""
    if isinstance(value, list):
        return [resolve_references(item, rows) for item in value]
    if isinstance(value, dict):
        return {k: resolve_references(v, rows) for k, v in value.items()}
    if isinstance(value, str) and value.startswith("$"):
        if value.startswith("$$"):
            return value[1:]
        match = REGEX_ROW_REFERENCE.match(value)
        if match is None:
            raise ValueError(f"Unsupported RSC reference: {value}")
        return rows[match.group(1)]
    return value


def collect_items(node: Any, items: dict[str, Any], rows: dict[str, Any]) -> None:
    """Map the "itemKey" of every "itemObject" on a company page to its value."""
    if isinstance(node, list):
        for child in node:
            collect_items(child, items, rows)
    elif isinstance(node, dict):
        item = node.get("itemObject")
        if item is not None:
            key = item["itemKey"]
            value = resolve_references(item["value"], rows)
            if key in items and items[key] != value:
                raise ValueError(f"Conflicting values for item: {key}")
            items[key] = value
        for child in node.values():
            collect_items(child, items, rows)


def fetch_items(context: Context, url: str) -> dict[str, Any]:
    # The RSC header asks Next.js for the server component stream instead of the
    # HTML page. KAP's cache ignores that header and can answer with a cached HTML
    # page, which the _rsc query parameter (as the Next.js client sets) avoids.
    # KAP throttles bulk page fetches, so the pages are fetched through Zyte and
    # cached for a few days, so that an interrupted run resumes.
    _, _, _, text = zyte_api.fetch_text(
        context,
        f"{url}?_rsc=1",
        headers={"RSC": "1"},
        cache_days=3,
        expected_media_type="text/x-component",
    )
    rows = parse_rsc_rows(text.encode("utf-8"))
    items: dict[str, Any] = {}
    collect_items(list(rows.values()), items, rows)
    return items


def parse_number(context: Context, value: str | None) -> str | None:
    """Parse a number in the source's Turkish format ("1.109.339.877,08") for a
    string property such as a percentage."""
    if value is None or value.strip() == "":
        return None
    result = context.lookup("number", value)
    if result is not None and result.value is not None:
        value = result.value
    number, unit = registry.number.parse(
        value,
        decimal=context.dataset.numbers.decimal,
        separator=context.dataset.numbers.separator,
    )
    if number is None or unit is not None:
        context.log.warning("Cannot parse number", value=value)
        return None
    return number


def apply_percentage(context: Context, ownership: Entity, value: str | None) -> None:
    percentage = parse_number(context, value)
    if percentage is not None and float(percentage) > 100:
        # Some filers swap the share value and ratio columns.
        context.log.info("Percentage out of range", ownership=ownership.id, value=value)
        return
    ownership.add("percentage", percentage)


def option(value: dict[str, str] | str | None, field: str) -> str | None:
    """Read a form field holding a selected option, such as
    {"key": "MALE", "text": "Erkek"}. Some filers typed free text instead."""
    if value is None or isinstance(value, str):
        return value
    return value[field]


def apply_tax_number(entity: Entity, value: str | None) -> None:
    """Turkish tax numbers (VKN) have 10 digits; foreign companies carry their
    own formats. 11-digit values are national identity numbers (TCKN) of natural
    persons and are dropped."""
    if value is None or REGEX_TCKN.match(value.strip()):
        return
    entity.add("taxNumber", value)


def crawl_shareholder(
    context: Context,
    company: Entity,
    row: dict[str, Any],
    member_ids: dict[str, str],
    ownership_type: str,
) -> None:
    name = row.pop("shareholder")
    ratio = row.pop("ratioInCapital")
    shares = row.pop("shareInCapital")
    currency = row.pop("monetaryUnit", None)
    context.audit_data(
        row, ignore=["votingRightRatio", "disableShareHolder", "hideDelete"]
    )
    # Rows for the remainder of the capital ("other", "publicly held") or the total.
    if name is None or context.lookup_value("shareholder", name) == "skip":
        return

    title = normalize_title(name)
    member_id = member_ids.get(title) if title is not None else None
    owner = context.make("LegalEntity")
    owner.id = member_id or context.make_id("shareholder", company.id, name)
    owner.add("name", name)
    if not owner.has("name"):
        return
    context.emit(owner)

    ownership = context.make("Ownership")
    ownership.id = context.make_id("ownership", company.id, owner.id, ownership_type)
    ownership.add("asset", company)
    ownership.add("owner", owner)
    ownership.add("ownershipType", ownership_type)
    apply_percentage(context, ownership, ratio)
    ownership.add("sharesValue", parse_number(context, shares))
    ownership.add("sharesCurrency", option(currency, "key"))
    context.emit(ownership)


def crawl_board_member(context: Context, company: Entity, row: dict[str, Any]) -> None:
    name = row.pop("nameSurname")
    representative_name = row.pop("personWhoBehavesOnBehalfOfLegalPersonBoardMember")
    gender = row.pop("gender")
    profession = row.pop("profession")
    if name is None or name.strip() == "":
        # The filer entered the member in the representative column.
        name, representative_name = representative_name, None

    # A legal person on the board names the natural person who acts for it.
    # Foreign natural persons are sometimes listed as their own representative,
    # and placeholders in the column are dropped by the type.name lookup.
    representative = context.make("Person")
    representative.id = context.make_id(
        "representative", company.id, name, representative_name
    )
    if normalize_title(representative_name) != normalize_title(name):
        representative.add("name", representative_name)
    is_legal_person = representative.has("name")

    director = context.make("LegalEntity" if is_legal_person else "Person")
    director.id = context.make_id("director", company.id, name)
    director.add("name", name)
    if not director.has("name"):
        context.audit_data(row, ignore=BOARD_IGNORE)
        return
    person = representative if is_legal_person else director
    # The gender and profession describe the natural person on the board.
    person.add("gender", option(gender, "key"))
    person.add("profession", option(profession, "text"))
    context.emit(director)

    directorship = context.make("Directorship")
    directorship.id = context.make_id("directorship", company.id, director.id)
    directorship.add("organization", company)
    directorship.add("director", director)
    title = row.pop("title")
    directorship.add("role", option(title, "text"))
    h.apply_date(directorship, "startDate", row.pop("firstChosenDate", None))
    for key in ("independentBoardMemberOrNot", "executiveOrNon"):
        directorship.add("description", option(row.pop(key, None), "text"))
    context.emit(directorship)

    if person is not director:
        context.emit(person)
        representation = context.make("Representation")
        representation.id = context.make_id("representation", director.id, person.id)
        representation.add("agent", person)
        representation.add("client", director)
        representation.add("role", "Tüzel kişi yönetim kurulu üyesi adına hareket eden")
        context.emit(representation)
    context.audit_data(row, ignore=BOARD_IGNORE)


def crawl_subsidiary(context: Context, company: Entity, row: dict[str, Any]) -> None:
    name = row.pop("companyTitle")
    lei_code = row.pop("leiCode")
    lei = LEI.normalize(lei_code) if lei_code is not None else None
    subsidiary = context.make("Company")
    if lei is not None:
        subsidiary.id = context.make_slug(lei, prefix="lei")
    else:
        subsidiary.id = context.make_id("subsidiary", company.id, name)
    # Placeholders for "no subsidiaries" are dropped by the type.name lookup.
    subsidiary.add("name", REGEX_NUMBERED.sub("", name) if name is not None else None)
    if not subsidiary.has("name"):
        return
    subsidiary.add("leiCode", lei_code)
    apply_tax_number(subsidiary, row.pop("taxNo"))
    subsidiary.add("sector", row.pop("scopeOfActivitiesOfCompany"))
    currency = option(row.pop("monetaryUnit"), "key")
    capital = row.pop("paidInOrIssuedCapital")
    if capital is not None:
        h.apply_number(subsidiary, "capital", capital)
        subsidiary.add("currency", currency)
    context.emit(subsidiary)

    ownership = context.make("Ownership")
    ownership.id = context.make_id("ownership", company.id, subsidiary.id)
    ownership.add("owner", company)
    ownership.add("asset", subsidiary)
    apply_percentage(context, ownership, row.pop("ratioOfCapitalShareOfCompany"))
    ownership.add(
        "sharesValue", parse_number(context, row.pop("capitalShareOfCompany"))
    )
    ownership.add("sharesCurrency", currency)
    ownership.add("role", row.pop("relationWithTheCompany"))
    context.emit(ownership)
    context.audit_data(row)


def crawl_items(
    context: Context,
    company: Entity,
    items: dict[str, Any],
    member_ids: dict[str, str],
) -> None:
    company.add("alias", items.pop("kpy41_acc1_isletme_adi", None))
    company.add("sector", items.pop("kpy41_acc2_sektor", None))
    # Operating status, reported under one of two keys depending on member type.
    company.add("status", items.pop("kpy41_acc2_faaliyet_durum", None))
    company.add("status", items.pop("kpy41_acc2_faaliyet_durum_2", None))
    company.add("website", items.pop("kpy41_acc1_int_addres"))
    for row in items.pop("kpy41_acc1_ilet_email") or []:
        company.add("email", row.pop("email"))
        context.audit_data(row)

    # Listed companies disclose holders of 5% or more, directly and indirectly;
    # other member types disclose their full shareholding structure.
    for row in items.pop("kpy41_acc5_sermayede_dogrudan", None) or []:
        crawl_shareholder(context, company, row, member_ids, "direct")
    for row in items.pop("kpy41_acc5_ortaklik_yapisi", None) or []:
        crawl_shareholder(context, company, row, member_ids, "direct")
    for row in items.pop("kpy41_acc5_son_durum_sermayeye", None) or []:
        crawl_shareholder(context, company, row, member_ids, "indirect")
    for key in (
        "kpy41_acc6_yonetim_kurulu_uyeleri",
        "kpy41_acc6_yonetim_kurulu_uyeleri_2",
    ):
        for row in items.pop(key, None) or []:
            crawl_board_member(context, company, row)
    for row in items.pop("kpy41_acc7_bagli_ortakliklar", None) or []:
        crawl_subsidiary(context, company, row)
    context.audit_data(items, ignore=ITEMS_IGNORE)


def normalize_title(name: str | None) -> str | None:
    """Fold case and diacritics, so that "Koç Holding A.Ş." matches the member
    title "KOÇ HOLDİNG A.Ş."."""
    return normalize(name, lowercase=True, ascii=True)


def crawl(context: Context) -> None:
    members: list[dict[str, Any]] = []
    for member in zyte_api.fetch_json(context, context.data_url):
        # KAP's own test accounts, and a trading system registered as a member.
        if context.lookup_value("member_skip", member["kapMemberTitle"]) == "skip":
            continue
        members.append(member)

    # Shareholders are linked to a KAP member when the name is that member's title.
    member_ids: dict[str, str] = {}
    ambiguous: set[str] = set()
    for member in members:
        title = normalize_title(member["kapMemberTitle"])
        if title is None:
            continue
        if title in member_ids:
            ambiguous.add(title)
        member_id = context.make_slug(member["companyCode"])
        assert member_id is not None, member
        member_ids[title] = member_id
    for title in ambiguous:
        member_ids.pop(title)

    for member in members:
        crawl_member(context, member, member_ids)


def crawl_member(
    context: Context, member: dict[str, Any], member_ids: dict[str, str]
) -> None:
    name = member.pop("kapMemberTitle")
    # Most members are companies; the regulator and the industry association are not.
    schema = context.lookup_value("member_schema", name, "Company")
    assert schema is not None
    company = context.make(schema)
    company.id = context.make_slug(member.pop("companyCode"))
    company.add("name", name)
    company.add("jurisdiction", "tr")
    apply_tax_number(company, member.pop("taxNo"))
    company.add("registrationNumber", member.pop("tradeRegNo"))
    h.apply_date(company, "incorporationDate", member.pop("tradeRegDate"))
    paid_capital = member.pop("paidCapital")
    if paid_capital is not None:
        h.apply_number(company, "capital", paid_capital)
        company.add("currency", "TRY")

    # Shares of the member are traded on Borsa İstanbul. The member codes are
    # its share tickers and, for banks and brokers, also its three-letter
    # exchange member code.
    codes = h.multi_split(member.pop("stockCode"), [","])
    if member.pop("payIslemDurumu") == "1":
        company.add("topics", "corp.public")
        for code in codes:
            if len(code) > 3:
                company.add("ticker", code)

    state = member.pop("kapMemberState")
    if state != "A":
        context.log.warning("Unknown member state", state=state, company=company.id)

    # Member types without a general information page: issuers of capital market
    # instruments that are not traded on the exchange (IGMS), other members (DG)
    # and market institutions such as the exchange and the regulator (DDK).
    member_type = member.pop("kapMemberType")
    full_address = None
    if member_type not in ("IGMS", "DG", "DDK"):
        url = f"https://www.kap.org.tr/tr/sirket-bilgileri/genel/{member.pop('mkkMemberOid')}"
        items = fetch_items(context, url)
        if len(items) == 0:
            context.log.warning(
                "No company data on general information page",
                url=url,
                member_type=member_type,
            )
        else:
            company.add("sourceUrl", url)
            full_address = items.pop("kpy41_acc1_merkez_adresi")
            crawl_items(context, company, items, member_ids)
    # The headquarters address from the page, or the city from the company list.
    city = member.pop("cityName")
    if full_address is not None and full_address.strip() != "":
        address = h.make_address(context, full=full_address, country_code="tr")
    else:
        address = h.make_address(context, city=city, country_code="tr")
    h.copy_address(company, address)
    context.emit(company)

    context.audit_data(
        member,
        ignore=[
            "kapMemberOid",
            "mkkMemberOid",
            "kapTypes",
            "financialType",
            "nonInactiveCount",
            "abcdCode",
            "sgbfOrtaklikYapisi",
            "faaliyetDurumu",
            "taxOffice",
            "tradeRegOffice",
            # The registered capital ceiling has no FollowTheMoney equivalent.
            "kayitliSermayeTavani",
            # The member's independent auditor, or the member itself for audit firms.
            "relatedMemberOid",
            "relatedMemberTitle",
        ],
    )
