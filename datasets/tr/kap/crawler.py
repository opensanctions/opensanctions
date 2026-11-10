import json
import re
from dataclasses import dataclass, field
from typing import Any

from followthemoney import registry
from normality import normalize
from rigour.ids import LEI

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api

OFFICER_IGNORE = [
    # The types of the identity documents of the officer and of the natural person
    # acting for a legal person officer. The page carries no numbers for them.
    "credentialKey",
    "credentialKey2",
    "positionsHeldInTheCompanyInTheLastFiveYears",
    "currentPositionsHeldOutsideTheCompany",
    "fiveYearsExperience",
    "shareInCapital",
    "theShareGroupThatTheBoardMemberRepresenting",
    "independentBoardMemberOrNot",
    "executiveOrNon",
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
    # Portfolio managers, other staff, partners of audit firms and the
    # companies an audit firm audits.
    "kpy41_acc6_portfoy_yoneticileri",
    "kpy41_acc6_sermaye_piyasa_faaliyetleri",
    "kpy41_acc6_sirketin_diger_personel",
    "kpy41_acc6_birimler_hizmetler",
    "kpy41_acc6_sorumlu_ortaklar",
    "kpy41_acc9_son_durum_denetledigi_kap",
]


@dataclass
class Members:
    """Entity IDs of the KAP members, to link shareholders and subsidiaries to them."""

    by_title: dict[str, str] = field(default_factory=dict)
    by_tax_number: dict[str, str] = field(default_factory=dict)


def parse_rsc_rows(stream: bytes) -> dict[str, Any]:
    """Split a React Server Components stream into its rows, keyed by row ID.

    Each row is "<hex id>:<payload>". A payload is a single line of JSON, except
    for text rows, "T<hex byte length>,<text>", whose text can span lines. Module
    ("I"), resource hint ("HL") and stream start and close ("X", "C") rows carry
    no data and are skipped.
    """
    rows: dict[str, Any] = {}
    pos = 0
    while pos < len(stream):
        match = re.compile(rb"([0-9a-f]*):").match(stream, pos)
        if match is None:
            raise ValueError(
                f"Invalid RSC row at byte {pos}: {stream[pos : pos + 50]!r}"
            )
        row_id = match.group(1).decode("ascii")
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
        if payload.startswith((b"I[", b"HL[")) or payload in (b"X", b"C"):
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
        match = re.fullmatch(r"\$([0-9a-f]+)", value)
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
    # Pages are cached for a few days, so that an interrupted run resumes.
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


def pop_table(
    context: Context, company: Entity, items: dict[str, Any], key: str
) -> list[dict[str, Any]]:
    """Pop the rows of a table on a company page. Some filers typed text into the
    table instead of rows."""
    value = items.pop(key, None)
    if value is None:
        return []
    if isinstance(value, str):
        if context.lookup_value("table_text", value) != "skip":
            context.log.warning(
                "Text in a table", company=company.id, key=key, value=value
            )
        return []
    rows: list[dict[str, Any]] = value
    assert all(isinstance(row, dict) for row in rows), (company.id, key, value)
    return rows


def parse_number(context: Context, value: str | None) -> str | None:
    """Parse a number in the source's Turkish format ("1.109.339.877,08") for a
    string property such as a percentage."""
    if value is None or value.strip() == "":
        return None
    value = context.lookup_value("number", value, value)
    assert value is not None
    number, unit = registry.number.parse(
        value,
        decimal=context.dataset.numbers.decimal,
        separator=context.dataset.numbers.separator,
    )
    if number is None or unit is not None:
        context.log.warning("Cannot parse number", value=value)
        return None
    return number


def make_ownership(
    context: Context,
    owner: str,
    asset: str,
    ownership_type: str | None,
    ratio: str | None,
    shares: str | None,
    currency: str | None,
) -> Entity:
    ownership = context.make("Ownership")
    ownership.id = context.make_id("ownership", owner, asset, ownership_type)
    ownership.add("ownershipType", ownership_type)
    ownership.add("owner", owner)
    ownership.add("asset", asset)
    percentage = parse_number(context, ratio)
    if percentage is not None and float(percentage) > 100:
        # Some filers swap the share value and ratio columns.
        context.log.info("Percentage out of range", asset=asset, value=ratio)
    else:
        ownership.add("percentage", percentage)
    ownership.add("sharesValue", parse_number(context, shares))
    ownership.add("sharesCurrency", currency)
    return ownership


def option(value: dict[str, str] | str | None, field: str) -> str | None:
    """Read a form field holding a selected option, such as
    {"key": "MALE", "text": "Erkek"}. Some filers typed free text instead."""
    if value is None or isinstance(value, str):
        return value
    return value[field]


def apply_tax_number(entity: Entity, value: str | None) -> None:
    """Add a tax number unless it is the national identity number (TCKN) of a
    natural person."""
    if value is None or re.fullmatch(r"\d{11}", value.strip()):
        return
    entity.add("taxNumber", value)


def normalize_title(name: str | None) -> str | None:
    """Fold case and diacritics, so that "Koç Holding A.Ş." matches the member
    title "KOÇ HOLDİNG A.Ş."."""
    return normalize(name, lowercase=True, ascii=True)


def crawl_shareholder(
    context: Context,
    company: Entity,
    row: dict[str, Any],
    members: Members,
    ownership_type: str,
) -> None:
    name = row.pop("shareholder")
    ratio = row.pop("ratioInCapital")
    shares = row.pop("shareInCapital")
    currency = option(row.pop("monetaryUnit", None), "key")
    context.audit_data(
        row, ignore=["votingRightRatio", "disableShareHolder", "hideDelete"]
    )
    # Rows for the remainder of the capital ("other", "publicly held") or the total.
    if name is None or context.lookup_value("shareholder", name) == "skip":
        return

    title = normalize_title(name)
    owner_id = members.by_title.get(title) if title is not None else None
    if owner_id is None:
        owner = context.make("LegalEntity")
        owner.id = context.make_id("shareholder", company.id, name)
        owner.add("name", name)
        if not owner.has("name"):
            return
        context.emit(owner)
        owner_id = owner.id

    assert company.id is not None and owner_id is not None
    ownership = make_ownership(
        context, owner_id, company.id, ownership_type, ratio, shares, currency
    )
    context.emit(ownership)


def crawl_officer(context: Context, company: Entity, row: dict[str, Any]) -> None:
    name = row.pop("nameSurname")
    agent_name = row.pop("personWhoBehavesOnBehalfOfLegalPersonBoardMember", None)
    if name is None or name.strip() == "":
        # The filer entered the member in the representative column.
        name, agent_name = agent_name, None
    # A legal person on the board names the natural person who acts for it.
    # Foreign natural persons are sometimes listed as acting for themselves, and
    # placeholders in the column are dropped by the type.name lookup.
    agent = context.make("Person")
    agent.id = context.make_id("representative", company.id, name, agent_name)
    if normalize_title(agent_name) != normalize_title(name):
        agent.add("name", agent_name)

    officer = context.make("LegalEntity" if agent.has("name") else "Person")
    officer.id = context.make_id("director", company.id, name)
    officer.add("name", name)
    if not officer.has("name"):
        context.audit_data(row, ignore=OFFICER_IGNORE)
        return
    # The gender and profession describe the natural person.
    person = agent if agent.has("name") else officer
    person.add("gender", option(row.pop("gender", None), "key"))
    person.add("profession", option(row.pop("profession"), "text"))
    context.emit(officer)

    role = option(row.pop("title"), "text")
    directorship = context.make("Directorship")
    directorship.id = context.make_id("directorship", company.id, officer.id, role)
    directorship.add("organization", company)
    directorship.add("director", officer)
    directorship.add("role", role)
    h.apply_date(directorship, "startDate", row.pop("firstChosenDate", None))
    context.emit(directorship)
    context.audit_data(row, ignore=OFFICER_IGNORE)

    if person is agent:
        context.emit(agent)
        representation = context.make("Representation")
        representation.id = context.make_id("representation", officer.id, agent.id)
        representation.add("agent", agent)
        representation.add("client", officer)
        context.emit(representation)


def crawl_subsidiary(
    context: Context, company: Entity, row: dict[str, Any], members: Members
) -> None:
    name = row.pop("companyTitle")
    tax_number = row.pop("taxNo")
    lei_code = row.pop("leiCode")
    scope = row.pop("scopeOfActivitiesOfCompany")
    capital = row.pop("paidInOrIssuedCapital")
    currency = option(row.pop("monetaryUnit"), "key")
    ratio = row.pop("ratioOfCapitalShareOfCompany")
    shares = row.pop("capitalShareOfCompany")
    relation = row.pop("relationWithTheCompany")
    context.audit_data(row)

    # A subsidiary that is itself a KAP member is linked to the member.
    subsidiary_id = members.by_tax_number.get(tax_number) if tax_number else None
    if subsidiary_id is None:
        subsidiary = context.make("Company")
        lei = LEI.normalize(lei_code) if lei_code is not None else None
        if lei is not None:
            subsidiary.id = context.make_slug(lei, prefix="lei")
        else:
            subsidiary.id = context.make_id("subsidiary", company.id, name)
        # Placeholders for "no subsidiaries" are dropped by the type.name lookup.
        if name is not None:
            # Filers sometimes number the names: "7) Agrotech USA LLC".
            subsidiary.add("name", re.sub(r"^\d+\)\s*", "", name))
        if not subsidiary.has("name"):
            return
        subsidiary.add("leiCode", lei_code)
        apply_tax_number(subsidiary, tax_number)
        subsidiary.add("sector", scope)
        if capital is not None:
            h.apply_number(subsidiary, "capital", capital)
            subsidiary.add("currency", currency)
        context.emit(subsidiary)
        subsidiary_id = subsidiary.id

    assert company.id is not None and subsidiary_id is not None
    ownership = make_ownership(
        context, company.id, subsidiary_id, None, ratio, shares, currency
    )
    ownership.add("role", relation)
    context.emit(ownership)


def crawl_items(
    context: Context, company: Entity, items: dict[str, Any], members: Members
) -> None:
    company.add("alias", items.pop("kpy41_acc1_isletme_adi", None))
    company.add("sector", items.pop("kpy41_acc2_sektor", None))
    # Operating status, reported under one of two keys depending on member type.
    company.add("status", items.pop("kpy41_acc2_faaliyet_durum", None))
    company.add("status", items.pop("kpy41_acc2_faaliyet_durum_2", None))
    company.add("website", items.pop("kpy41_acc1_int_addres"))
    for row in pop_table(context, company, items, "kpy41_acc1_ilet_email"):
        company.add("email", row.pop("email"))
        context.audit_data(row)

    # Listed companies disclose holders of 5% or more, directly and indirectly;
    # other member types disclose their full shareholding structure.
    for key, ownership_type in (
        ("kpy41_acc5_sermayede_dogrudan", "direct"),
        ("kpy41_acc5_ortaklik_yapisi", "direct"),
        ("kpy41_acc5_son_durum_sermayeye", "indirect"),
    ):
        for row in pop_table(context, company, items, key):
            crawl_shareholder(context, company, row, members, ownership_type)
    # Board members, and senior managers with a say in the management.
    for key in (
        "kpy41_acc6_yonetim_kurulu_uyeleri",
        "kpy41_acc6_yonetim_kurulu_uyeleri_2",
        "kpy41_acc6_yonetimde_soz_sahibi",
    ):
        for row in pop_table(context, company, items, key):
            crawl_officer(context, company, row)
    for row in pop_table(context, company, items, "kpy41_acc7_bagli_ortakliklar"):
        crawl_subsidiary(context, company, row, members)
    context.audit_data(items, ignore=ITEMS_IGNORE)


def crawl_member(context: Context, member: dict[str, Any], members: Members) -> None:
    name = member.pop("kapMemberTitle")
    # Most members are companies; the regulator and the industry bodies are not.
    schema = context.lookup_value("member_schema", name, "Company")
    assert schema is not None
    company = context.make(schema)
    company.id = context.make_slug(member.pop("companyCode"))
    company.add("name", name)
    company.add("jurisdiction", "tr")
    apply_tax_number(company, member.pop("taxNo"))
    # Trade registry numbers are only unique within a registry office, so they
    # are emitted with the office, or not at all.
    reg_office = member.pop("tradeRegOffice")
    reg_no = member.pop("tradeRegNo")
    reg_no = context.lookup_value("trade_reg_no", reg_no, reg_no)
    if reg_office is not None and reg_no is not None:
        company.add("registrationNumber", f"{reg_office} {reg_no}")
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

    member_type = member.pop("kapMemberType")
    page = context.lookup("member_type", member_type)
    if page is None:
        raise ValueError(f"Unknown member type: {member_type} ({name})")
    full_address = None
    if page.value == "page":
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
            crawl_items(context, company, items, members)
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
            # The registered capital ceiling has no FollowTheMoney equivalent.
            "kayitliSermayeTavani",
            # The member's independent auditor, or the member itself for audit firms.
            "relatedMemberOid",
            "relatedMemberTitle",
        ],
    )


def index_members(context: Context, members: list[dict[str, Any]]) -> Members:
    """Map member titles and tax numbers to member entity IDs, leaving out the
    ones that more than one member shares."""
    index = Members()
    ambiguous: set[str] = set()
    for member in members:
        member_id = context.make_slug(member["companyCode"])
        assert member_id is not None, member
        title = normalize_title(member["kapMemberTitle"])
        for key, mapping in (
            (title, index.by_title),
            (member["taxNo"], index.by_tax_number),
        ):
            if key is None or key == "":
                continue
            if key in mapping:
                ambiguous.add(key)
            mapping[key] = member_id
    for key in ambiguous:
        index.by_title.pop(key, None)
        index.by_tax_number.pop(key, None)
    return index


def crawl(context: Context) -> None:
    members: list[dict[str, Any]] = []
    for member in zyte_api.fetch_json(context, context.data_url):
        if context.lookup_value("member_skip", member["kapMemberTitle"]) == "skip":
            continue
        members.append(member)
    index = index_members(context, members)
    for member in members:
        crawl_member(context, member, index)
