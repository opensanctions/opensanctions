import json
import re
from typing import Any

from rigour.ids import LEI

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api

# Member types that have no general information page on KAP: issuers of
# capital market instruments that are not traded on the exchange, and other
# members. Their pages return a not-found template with a 200 status.
TYPES_WITHOUT_PAGE = ("IGMS", "DG")

# KAP is a Next.js application that ships the full page data as a React Server
# Components "flight" payload: a series of self.__next_f.push([1, "..."]) calls
# whose string arguments concatenate into one JSON-like stream. The company
# data is read from that stream rather than from the rendered HTML.
REGEX_FLIGHT_CHUNK = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', re.DOTALL)
REGEX_ITEM_OBJECT = re.compile(r'"itemObject":\{')
# Subsidiary names are sometimes numbered by the filer: "7) Agrotech USA LLC".
REGEX_NUMBERED = re.compile(r"^\d+\)\s*")
REGEX_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
# A domain name or URL: at least one dot, no whitespace or brackets.
REGEX_WEBSITE = re.compile(r"^(https?://)?[\w-]+(\.[\w-]+)+(/\S*)?$", re.IGNORECASE)
# 11-digit values in a tax number field are national identity numbers (TCKN)
# of natural persons, not the 10-digit Turkish tax number (VKN).
REGEX_TCKN = re.compile(r"^\d{11}$")

# Keys in board member records that carry national identity or tax numbers.
# They must never be emitted, so they are consumed and dropped explicitly.
IDENTITY_KEYS = [
    "tcknYkn",
    "tcknYknVkn",
    "personWhoBehavesOnBehalfOfLegalPersonBoardMemberTcknVknYkn",
    "credentialKey",
    "credentialKey2",
]
BOARD_IGNORE = [
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
    "styleName",
]
SHAREHOLDER_IGNORE = ["disableShareHolder", "hideDelete", "votingRightRatio"]
SUBSIDIARY_IGNORE = ["monetaryUnit"]


def flight_payload(html: str) -> str:
    """Concatenate the JSON-encoded flight chunks of a Next.js page."""
    parts = [json.loads(f'"{chunk}"') for chunk in REGEX_FLIGHT_CHUNK.findall(html)]
    return "".join(parts)


def parse_json_at(text: str, start: int) -> Any:
    """Parse the JSON object or array that begins at ``start``."""
    depth = 0
    for idx in range(start, len(text)):
        char = text[idx]
        if char in "[{":
            depth += 1
        elif char in "]}":
            depth -= 1
            if depth == 0:
                return json.loads(text[start : idx + 1])
    raise ValueError("Unterminated JSON value")


def json_after_key(text: str, key: str) -> Any:
    """Parse the JSON value that follows the first occurrence of ``"key":``."""
    marker = f'"{key}":'
    idx = text.find(marker)
    if idx < 0:
        raise ValueError(f"Key not found in payload: {key}")
    return parse_json_at(text, idx + len(marker))


def parse_items(payload: str) -> dict[str, Any]:
    """Map each ``itemKey`` on a company page to its ``value``."""
    items: dict[str, Any] = {}
    for match in REGEX_ITEM_OBJECT.finditer(payload):
        obj = parse_json_at(payload, match.end() - 1)
        key = obj.get("itemKey")
        if key is not None and key not in items:
            items[key] = obj.get("value")
    return items


def clean(value: Any) -> str | None:
    """Strip a source string, treating filer placeholders ("-", "---", "YOK",
    meaning none) as empty."""
    if not isinstance(value, str):
        return None
    text: str = value.strip()
    if text == "" or set(text) == {"-"} or text.casefold() == "yok":
        return None
    return text


def keyed_text(value: Any) -> str | None:
    """Return the display text of a KAP ``{"key": ..., "text": ...}`` value."""
    if isinstance(value, dict):
        return clean(value.get("text"))
    return clean(value)


def keyed_key(value: Any) -> str | None:
    if isinstance(value, dict):
        return clean(value.get("key"))
    return None


def clean_percent(value: Any) -> str | None:
    """Turn a Turkish-formatted percentage ("43,75") into "43.75"."""
    text = clean(value)
    if text is None:
        return None
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    return text


def apply_tax_number(entity: Entity, value: Any) -> None:
    """Add a tax number. Turkish tax numbers (VKN) have 10 digits; foreign
    subsidiaries carry their own formats. 11-digit values are national identity
    numbers (TCKN) of natural persons and are dropped."""
    tax_number = clean(value)
    if tax_number is None or REGEX_TCKN.match(tax_number):
        return
    entity.add("taxNumber", tax_number)


def apply_contact(company: Entity, items: dict[str, Any]) -> None:
    for row in items.get("kpy41_acc1_ilet_email") or []:
        email = clean(row.get("email"))
        if email is None:
            continue
        # Some filers label the address ("GENEL MÜDÜRLÜK : info@example.com")
        # or list several; others put their contact form URL in the column.
        if "@" in email:
            company.add("email", REGEX_EMAIL.findall(email))
        else:
            company.add("website", email)
    # The website field is free text: several URLs separated by ";" or ",",
    # sometimes followed by the company name in brackets. Keep the URL-shaped
    # tokens only.
    website = clean(items.get("kpy41_acc1_int_addres"))
    if website is not None:
        for token in h.multi_split(website, [";", ",", " "]):
            if REGEX_WEBSITE.match(token):
                company.add("website", token)


def crawl_shareholder(
    context: Context,
    company: Entity,
    row: dict[str, Any],
    members: dict[str, str],
    ownership_type: str,
) -> None:
    name = clean(row.pop("shareholder", None))
    if name is None:
        return
    if context.lookup_value("shareholder", name) == "skip":
        context.audit_data(
            row,
            ignore=SHAREHOLDER_IGNORE
            + ["shareInCapital", "ratioInCapital", "monetaryUnit"],
        )
        return

    member_id = members.get(name.casefold())
    if member_id is not None:
        owner = context.make("Company")
        owner.id = member_id
    else:
        owner = context.make("LegalEntity")
        owner.id = context.make_id("shareholder", company.id, name)
    owner.add("name", name)
    context.emit(owner)

    ownership = context.make("Ownership")
    ownership.id = context.make_id("ownership", company.id, owner.id, ownership_type)
    ownership.add("asset", company)
    ownership.add("owner", owner)
    ownership.add("ownershipType", ownership_type)
    ownership.add("percentage", clean_percent(row.pop("ratioInCapital", None)))
    ownership.add("sharesValue", clean(row.pop("shareInCapital", None)))
    ownership.add("sharesCurrency", keyed_key(row.pop("monetaryUnit", None)))
    context.emit(ownership)
    context.audit_data(row, ignore=SHAREHOLDER_IGNORE)


def crawl_board_member(context: Context, company: Entity, row: dict[str, Any]) -> None:
    name = clean(row.pop("nameSurname", None))
    if name is None:
        return
    for key in IDENTITY_KEYS:
        row.pop(key, None)
    representative = clean(
        row.pop("personWhoBehavesOnBehalfOfLegalPersonBoardMember", None)
    )
    gender = keyed_key(row.pop("gender", None))
    profession = keyed_text(row.pop("profession", None))

    if representative is not None:
        # A legal person holds the seat and a natural person acts on its behalf;
        # the gender and profession fields then describe the representative.
        director = context.make("LegalEntity")
        director.id = context.make_id("director", company.id, name)
        director.add("name", name)
    else:
        director = context.make("Person")
        director.id = context.make_id("director", company.id, name)
        director.add("name", name)
        director.add("gender", gender)
        director.add("profession", profession)
    context.emit(director)

    directorship = context.make("Directorship")
    directorship.id = context.make_id("directorship", company.id, director.id)
    directorship.add("organization", company)
    directorship.add("director", director)
    directorship.add("role", keyed_text(row.pop("title", None)))
    h.apply_date(directorship, "startDate", clean(row.pop("firstChosenDate", None)))
    for key in ("independentBoardMemberOrNot", "executiveOrNon"):
        directorship.add("description", keyed_text(row.pop(key, None)))
    if representative is not None:
        directorship.add("description", f"Temsilci: {representative}")
    context.emit(directorship)
    context.audit_data(row, ignore=BOARD_IGNORE)


def crawl_subsidiary(context: Context, company: Entity, row: dict[str, Any]) -> None:
    name = clean(row.pop("companyTitle", None))
    if name is None:
        return
    name = REGEX_NUMBERED.sub("", name)
    lei_raw = clean(row.pop("leiCode", None))
    lei = LEI.normalize(lei_raw) if lei_raw is not None else None
    if lei_raw is not None and lei is None:
        context.log.warning("Invalid LEI", lei=lei_raw, company=company.id)

    subsidiary = context.make("Company")
    if lei is not None:
        subsidiary.id = f"lei-{lei}"
    else:
        subsidiary.id = context.make_id("subsidiary", company.id, name)
    subsidiary.add("name", name)
    subsidiary.add("leiCode", lei)
    apply_tax_number(subsidiary, row.pop("taxNo", None))
    subsidiary.add("sector", clean(row.pop("scopeOfActivitiesOfCompany", None)))
    subsidiary.add("capital", clean(row.pop("paidInOrIssuedCapital", None)))
    context.emit(subsidiary)

    ownership = context.make("Ownership")
    ownership.id = context.make_id("ownership", company.id, subsidiary.id)
    ownership.add("owner", company)
    ownership.add("asset", subsidiary)
    ownership.add(
        "percentage", clean_percent(row.pop("ratioOfCapitalShareOfCompany", None))
    )
    ownership.add("sharesValue", clean(row.pop("capitalShareOfCompany", None)))
    ownership.add("role", clean(row.pop("relationWithTheCompany", None)))
    context.emit(ownership)
    context.audit_data(row, ignore=SUBSIDIARY_IGNORE)


def crawl_company_page(
    context: Context,
    company: Entity,
    url: str,
    members: dict[str, str],
) -> bool:
    """Fill the company from its general information page. Returns False if
    the page carries no company data."""
    # KAP throttles direct requests after a few hundred pages, so the pages
    # are fetched through the Zyte API. Pages are cached for a few days so
    # that a run interrupted by a network error resumes rather than restarts.
    _, _, _, html = zyte_api.fetch_text(context, url, cache_days=3)
    payload = flight_payload(html)
    # The object enclosing the first "kapMemberTitle" carries the identifiers.
    title_at = payload.find('"kapMemberTitle":')
    if title_at < 0:
        return False
    header = parse_json_at(payload, payload.rfind("{", 0, title_at))
    items = parse_items(payload)

    company.add("sourceUrl", url)
    company.add("name", clean(header.get("kapMemberTitle")))
    apply_tax_number(company, header.get("taxNo"))
    company.add("registrationNumber", clean(header.get("tradeRegNo")))
    h.apply_date(company, "incorporationDate", clean(header.get("tradeRegDate")))
    company.add("ticker", clean(header.get("stockCode")))
    paid_capital = header.get("paidCapital")
    if paid_capital is not None:
        company.add("capital", str(paid_capital))
        company.add("currency", "TRY")

    company.add("alias", clean(items.get("kpy41_acc1_isletme_adi")))
    company.add("sector", clean(items.get("kpy41_acc2_sektor")))
    company.add("status", clean(items.get("kpy41_acc2_faaliyet_durum")))
    apply_contact(company, items)
    address = clean(items.get("kpy41_acc1_merkez_adresi"))
    if address is not None:
        addr = h.make_address(context, full=address, country_code="tr")
        h.copy_address(company, addr)

    # Listed companies disclose holders of 5% or more, directly and indirectly;
    # other member types disclose their full shareholding structure.
    for row in items.get("kpy41_acc5_sermayede_dogrudan") or []:
        crawl_shareholder(context, company, row, members, "direct")
    for row in items.get("kpy41_acc5_ortaklik_yapisi") or []:
        crawl_shareholder(context, company, row, members, "direct")
    for row in items.get("kpy41_acc5_son_durum_sermayeye") or []:
        crawl_shareholder(context, company, row, members, "indirect")
    for key in (
        "kpy41_acc6_yonetim_kurulu_uyeleri",
        "kpy41_acc6_yonetim_kurulu_uyeleri_2",
    ):
        for row in items.get(key) or []:
            crawl_board_member(context, company, row)
    for row in items.get("kpy41_acc7_bagli_ortakliklar") or []:
        crawl_subsidiary(context, company, row)
    return True


def crawl(context: Context) -> None:
    # The list is one request per run and is not affected by KAP's throttling
    # of bulk page fetches, and it is too large to fetch reliably through Zyte.
    html = context.fetch_text(context.data_url)
    if html is None:
        raise RuntimeError("Empty company list page")
    payload = flight_payload(html)
    groups = json_after_key(payload, "data")
    permalinks = json_after_key(payload, "companyPermaLinks")

    # Every member has exactly one numeric KAP code, which is the first part of
    # its permalink and is used as the entity ID.
    links: dict[str, str] = {}
    for link in permalinks:
        links.setdefault(link["mkkMemberOid"], link["permaLink"])

    members: list[dict[str, Any]] = [m for group in groups for m in group["content"]]
    if len(members) < 500:
        raise RuntimeError(f"Company list looks truncated: {len(members)} members")
    member_ids: dict[str, str] = {}
    for member in members:
        permalink = links.get(member["mkkMemberOid"])
        if permalink is None:
            context.log.warning("Member without permalink", member=member)
            continue
        member_id = context.make_slug(permalink.split("-", 1)[0])
        if member_id is None:
            context.log.warning("Cannot build member ID", permalink=permalink)
            continue
        member_ids[member["kapMemberTitle"].strip().casefold()] = member_id

    without_page = 0
    for member in members:
        permalink = links.get(member["mkkMemberOid"])
        if permalink is None:
            continue
        company = context.make("Company")
        company.id = context.make_slug(permalink.split("-", 1)[0])
        if company.id is None:
            continue
        company.add("name", clean(member.pop("kapMemberTitle")))
        company.add("ticker", clean(member.pop("stockCode", None)))
        company.add("jurisdiction", "tr")
        city = clean(member.pop("cityName", None))
        if city is not None:
            addr = h.make_address(context, city=city, country_code="tr")
            h.copy_address(company, addr)
        if company.get("ticker"):
            company.add("topics", "corp.public")

        member_type = member.pop("kapMemberType", None)
        url = f"https://www.kap.org.tr/tr/sirket-bilgileri/genel/{permalink}"
        if member_type in TYPES_WITHOUT_PAGE:
            without_page += 1
        elif not crawl_company_page(context, company, url, member_ids):
            context.log.warning(
                "No company data on general information page",
                url=url,
                member_type=member_type,
            )
        context.emit(company)
        context.audit_data(
            member, ignore=["mkkMemberOid", "relatedMemberOid", "relatedMemberTitle"]
        )
    context.log.info("Members without a general information page", count=without_page)
