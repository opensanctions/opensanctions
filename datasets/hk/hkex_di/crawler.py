import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

from followthemoney import registry
from lxml.etree import _Element as Element
from normality import normalize
from python_calamine import CalamineWorkbook
from requests.exceptions import RequestException

from zavod import Context, settings
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.review import assert_all_accepted

# Form tables read by the crawler, and those it does not: derivative interests,
# joint holders, trusts, concert parties (s.317) and the persons a corporate
# holder is accustomed to act for. An unknown table fails the form.
FORM_TABLES = {
    "grdSh_BEvt",
    "grdSh_AEvt",
    "grdCap_SS",
    "grdCap_Dir_Sh",
    "grdFI_Sh",
    "grdCtrlCorp",
    "grdDer_SS",
    "grdDer_Dir",
    "grdJI_Sh",
    "grdTrust_Sh",
    "grdPA_Sh",
    "grdPer_CorpRel",
}


@dataclass
class Listing:
    code: str
    name: str
    isin: str | None
    currency: str | None


@dataclass
class ShareClass:
    shareholders_url: str
    directors_url: str


@dataclass
class Issuer:
    sid: str
    name: str
    classes: list[ShareClass]


def fetch_page(
    context: Context, url: str, marker: str, cache_days: int | None = None
) -> Element:
    """Fetch a page, retrying with growing pauses after a connection error or
    while HKEX serves its error page instead. The marker is the ID of a label
    that only real pages carry."""
    for attempt in range(5):
        if attempt > 0:
            time.sleep(10 * 2**attempt)
        try:
            doc = context.fetch_html(url, cache_days=cache_days, absolute_links=True)
        except RequestException as exc:
            context.log.info("Request failed, retrying", url=url, error=str(exc))
            continue
        if len(h.xpath_elements(doc, f'//span[@id="{marker}"]')) == 1:
            return doc
        context.clear_url(url)
        context.log.info("Error page, retrying", url=url, attempt=attempt)
    raise ValueError(f"HKEX keeps failing: {url}")


def label(doc: Element, id: str) -> str | None:
    spans = h.xpath_elements(doc, f'//span[@id="{id}"]')
    if len(spans) == 0:
        return None
    return h.element_text(spans[0]) or None


def is_blank(cells: list[Element]) -> bool:
    """Tell whether a table row is empty, or holds only the "Nil" that some
    filers enter in the first cell of an empty table."""
    texts = [h.element_text(cell) for cell in cells]
    return all(text in ("", "Nil") for text in texts)


def table_rows(doc: Element, id: str) -> list[dict[str, Element]]:
    """Read a table whose header is its first row, skipping blank rows."""
    table = h.xpath_element(doc, f'//table[@id="{id}"]')
    return [
        row
        for row in h.parse_html_table(table, header_tag="td", index_empty_headers=True)
        if not is_blank(list(row.values()))
    ]


def nested_rows(doc: Element, id: str) -> list[list[Element]]:
    """Read a form table whose share counts sit in tables nested inside cells,
    which `h.parse_html_table` would read as rows of the outer table."""
    rows: list[list[Element]] = []
    for tr in h.xpath_elements(doc, f'//table[@id="{id}"]/tr')[1:]:
        cells = tr.findall("./td")
        if not is_blank(cells):
            rows.append(cells)
    return rows


def form_link(cell: Element) -> tuple[str, str] | None:
    """Return the form page (e.g. "NSForm2") and serial a list cell links to."""
    links = h.xpath_elements(cell, ".//a[@href]")
    if len(links) == 0:
        return None
    url = urlparse(links[0].get("href", ""))
    page = url.path.rsplit("/", 1)[-1].removesuffix(".aspx")
    return page, parse_qs(url.query)["fn"][0]


def name_key(context: Context, name: str | None) -> str | None:
    """Key a name so that rows referring to one party as "Mr. Bernard Chan
    Charnwut" and "Chan Bernard Charnwut" match."""
    norm = normalize(h.strip_name_titles(context, name), lowercase=True, ascii=True)
    if norm is None:
        return None
    return " ".join(sorted(norm.split()))


def reference_names(name: str) -> list[str]:
    """Return the forms under which other rows of a form may refer to a company:
    its name without a defined term ("Henderson Development Limited ("HD")") or a
    former name ("Prosus N.V. (formerly known as "Myriad ...")"), and the defined
    term itself. These only resolve references; the emitted names go through the
    name review."""
    names = [name]
    match = re.match(r"^(.*?)\s*\(\s*[\"“]([^\"”()]+)[\"”]\s*\)$", name)
    if match is not None:
        name = match.group(1)
        names.extend([name, match.group(2)])
    match = re.match(r"^(.*?)\s*\((?:formerly|previously) known as\s+.*\)$", name, re.I)
    if match is not None:
        names.append(match.group(1))
    return names


def split_place(text: str) -> tuple[str, str | None]:
    """Split an address into the address and the place of incorporation that
    the form appends in parentheses ("..., Grand Cayman (Cayman Islands)")."""
    match = re.match(r"^(.*?)\s*\(([^()]+)\)$", text, re.S)
    if match is None:
        return text, None
    return match.group(1), match.group(2)


def read_listings(context: Context, path: Path) -> list[list[Listing]]:
    """Read the equity securities, grouping the counters of one ISIN (such as an
    HKD and an RMB counter) so that each issuer is searched once."""
    wb = CalamineWorkbook.from_path(path)
    groups: dict[str, list[Listing]] = {}
    for row in h.parse_xlsx_sheet(context, wb.get_sheet_by_index(0), skiprows=2):
        if row.get("category") != "Equity":
            continue
        code = row.get("stock_code")
        name = row.get("name_of_securities")
        assert code is not None and name is not None, row
        isin = (row.get("isin") or "").strip() or None
        currency = row.get("trading_currency")
        listing = Listing(code=code, name=name, isin=isin, currency=currency)
        groups.setdefault(isin or code, []).append(listing)
    for group in groups.values():
        group.sort(key=lambda listing: listing.currency != "HKD")
    return list(groups.values())


def search_issuers(context: Context, code: str) -> list[Issuer]:
    assert context.dataset.url is not None
    # Part XV of the Securities and Futures Ordinance took effect on 1 April 2003.
    end_date = settings.RUN_TIME.strftime("%d/%m/%Y")
    url = urljoin(
        context.dataset.url,
        f"NSSrchCorpList.aspx?sa1=cl&scsd=01/04/2003&sced={end_date}&sc={code}"
        "&src=MAIN&lang=EN&g_lang=en",
    )
    doc = fetch_page(context, url, "lblRecCount")
    # An issuer with several share classes, such as H shares and domestic
    # shares, has a row per class: "Changmao ... Ltd.  - H Shares".
    rows: dict[str, list[tuple[str, ShareClass]]] = {}
    for row in table_rows(doc, "grdPaging"):
        links = h.links_to_dict(row.pop("report_type"))
        shareholders_url = links.get("consolidated_list_of_substantial_shareholders")
        directors_url = links.get("complete_list_of_directors")
        if shareholders_url is None or directors_url is None:
            context.log.warning("Search result without lists", code=code, url=url)
            continue
        sid = parse_qs(urlparse(shareholders_url).query)["sid"][0]
        name = h.element_text(row.pop("name_of_listed_corporation"))
        rows.setdefault(sid, []).append(
            (name, ShareClass(shareholders_url, directors_url))
        )
    issuers: list[Issuer] = []
    for sid, classes in rows.items():
        names = {name for name, _ in classes}
        if len(classes) > 1:
            names = {name.rsplit(" - ", 1)[0].strip() for name in names}
        if len(names) != 1:
            context.log.warning("Inconsistent issuer names", code=code, names=names)
        issuers.append(
            Issuer(sid=sid, name=min(names), classes=[c for _, c in classes])
        )
    return issuers


def list_rows(context: Context, url: str) -> list[dict[str, Element]]:
    """Read all pages of a shareholder or director list."""
    doc = fetch_page(context, url, "lblRecCount")
    count = label(doc, "lblRecCount")
    assert count is not None, url
    rows = table_rows(doc, "grdPaging")
    # Lists show 50 records per page.
    for page in range(2, (int(count) - 1) // 50 + 2):
        page_doc = fetch_page(context, f"{url.rstrip('&')}&pg={page}", "lblRecCount")
        rows.extend(table_rows(page_doc, "grdPaging"))
    if len(rows) != int(count):
        context.log.warning("List row count mismatch", url=url, rows=len(rows))
    return rows


def make_ownership(
    context: Context,
    owner: Entity,
    asset: Entity,
    shares: str | None,
    percentage: str | None,
) -> Entity:
    ownership = context.make("Ownership")
    ownership.id = context.make_id("ownership", owner.id, asset.id)
    ownership.add("owner", owner)
    ownership.add("asset", asset)
    if shares is not None:
        # sharesCount is a string property; store the count without separators.
        count, unit = registry.number.parse(shares)
        if count is None or unit is not None:
            context.log.warning("Cannot parse share count", value=shares)
        ownership.add("sharesCount", count)
    ownership.add("percentage", percentage)
    return ownership


def make_filer(
    context: Context, doc: Element, issuer: Issuer, kind: str, list_name: str
) -> tuple[Entity, list[str]]:
    """Make the entity that filed the form, with the names under which other
    rows of the form refer to it."""
    if kind == "NSForm2":
        raw_name = label(doc, "lblDName")
        assert raw_name is not None
        office = label(doc, "lblDRegOffice")
        filer = context.make("Company")
        filer.id = context.make_id("company", raw_name, office)
        h.apply_reviewed_name_string(context, filer, string=raw_name, llm_cleaning=True)
        filer.add("address", office)
        return filer, [*reference_names(raw_name), list_name]

    surname = label(doc, "lblDSurname")
    first = label(doc, "lblDFirstname")
    middle = label(doc, "lblDMidname")
    full = " ".join(p for p in (surname, first, middle) if p is not None)
    filer = context.make("Person")
    filer.id = context.make_id("person", issuer.sid, list_name)
    h.apply_name(
        filer, full=full, first_name=first, middle_name=middle, last_name=surname
    )
    filer.add("name", label(doc, "lblDChiName"), lang="zho")
    return filer, [full, list_name]


def crawl_controlled(
    context: Context,
    doc: Element,
    issuer_entity: Entity,
    filer: Entity,
    names: list[str],
    share_class: str | None,
    event_date: str | None,
    url: str,
) -> None:
    """Link the corporations through which the filer holds its interest to
    their controlling persons."""
    known: dict[str, Entity] = {}
    for name in names:
        key = name_key(context, name)
        if key is not None:
            known[key] = filer
    rows: list[tuple[Entity, str, str | None, bool, str | None]] = []
    for cells in nested_rows(doc, "grdCtrlCorp"):
        raw_name, address, controller, control, direct, shares = [
            h.element_text(c) for c in cells
        ]
        address, place = split_place(address)
        corp = context.make("Company")
        corp.id = context.make_id("company", raw_name, place)
        h.apply_reviewed_name_string(context, corp, string=raw_name, llm_cleaning=True)
        corp.add("address", address)
        if place is not None:
            # "Place of Incorporation: Hong Kong", "incorporated in the Netherlands"
            corp.add(
                "jurisdiction",
                re.sub(
                    r"^(?:place of incorporation\s*:|incorporated in)?\s*(?:the\s+)?",
                    "",
                    place,
                    flags=re.I,
                ),
            )
        corp.add("sourceUrl", url)
        for corp_name in reference_names(raw_name):
            key = name_key(context, corp_name)
            if key is not None:
                known.setdefault(key, corp)
        if direct not in ("Y", "N"):
            context.log.warning("Unknown direct interest flag", value=direct)
        long_shares = re.search(r"Long position\s*([\d,]+)", shares)
        rows.append(
            (
                corp,
                controller,
                control or None,
                direct == "Y",
                long_shares.group(1) if long_shares else None,
            )
        )

    for corp, controller, percent, is_direct, held in rows:
        context.emit(corp)
        # Some cells name several controlling parties: "Hezhou Company Limited
        # (as general partner); Tong Zhou Company Limited (as limited partner)".
        # The % control is theirs jointly, so their links carry no percentage.
        parties = [p.strip() for p in controller.split(";") if p.strip()]
        for party in parties:
            parent = resolve_controller(context, known, party, issuer_entity, url)
            if parent is None or parent.id == corp.id:
                continue
            joint = percent if len(parties) == 1 else None
            link = make_ownership(context, parent, corp, None, joint)
            link.add("sourceUrl", url)
            context.emit(link)
        if is_direct and held is not None:
            holding = make_ownership(context, corp, issuer_entity, held, None)
            holding.add("sharesType", share_class)
            h.apply_date(holding, "date", event_date)
            holding.add("sourceUrl", url)
            context.emit(holding)


def resolve_controller(
    context: Context,
    known: dict[str, Entity],
    controller: str,
    issuer_entity: Entity,
    url: str,
) -> Entity | None:
    """Find the entity a form row names as controlling person, or make one for
    a controller that no other row of the form describes, such as the trustee of
    a family trust."""
    key = name_key(context, controller)
    if key is None:
        return None
    # "Hezhou Company Limited (as general partner)" refers to the row for
    # "Hezhou Company Limited".
    role_key = name_key(context, re.sub(r"\s*\(as [^()]+\)$", "", controller))
    for candidate in (key, role_key):
        if candidate is not None and candidate in known:
            return known[candidate]
    context.log.info(
        "Controlling person not described in form",
        controller=controller,
        issuer=issuer_entity.id,
    )
    parent = context.make("LegalEntity")
    parent.id = context.make_id("controller", controller)
    h.apply_reviewed_name_string(
        context,
        parent,
        string=h.strip_name_titles(context, controller),
        llm_cleaning=True,
    )
    parent.add("sourceUrl", url)
    context.emit(parent)
    known[key] = parent
    return parent


def crawl_family(context: Context, doc: Element, filer: Entity, url: str) -> None:
    for cells in nested_rows(doc, "grdFI_Sh"):
        relation, name = h.element_text(cells[0]), h.element_text(cells[1])
        # Children under 18 are not named.
        if relation == "Child":
            continue
        if relation != "Spouse":
            context.log.warning("Unknown family relation", relation=relation)
            continue
        relative = context.make("Person")
        relative.id = context.make_id("relative", filer.id, name)
        relative.add("name", name)
        relative.add("sourceUrl", url)
        context.emit(relative)
        family = context.make("Family")
        family.id = context.make_id("family", filer.id, relative.id)
        family.add("person", filer)
        family.add("relative", relative)
        family.add("relationship", "spouse")
        family.add("sourceUrl", url)
        context.emit(family)


def crawl_form(
    context: Context,
    issuer: Issuer,
    issuer_entity: Entity,
    kind: str,
    serial: str,
    list_name: str,
) -> Entity:
    assert context.dataset.url is not None
    url = urljoin(context.dataset.url, f"{kind}.aspx?fn={serial}")
    # A form never changes: an amendment is filed as a new form with its own
    # serial, which appends "(Amendment to <serial>)" to the form's serial label.
    doc = fetch_page(context, url, "lblDSerialNo", cache_days=3650)
    form_serial = label(doc, "lblDSerialNo")
    assert form_serial is not None and form_serial.startswith(serial), url
    for table in h.xpath_elements(doc, '//table[starts-with(@id, "grd")]'):
        if table.get("id") not in FORM_TABLES:
            raise ValueError(f"Unknown table {table.get('id')}: {url}")

    filer, names = make_filer(context, doc, issuer, kind, list_name)
    filer.add("sourceUrl", url)
    context.emit(filer)

    share_class = label(doc, "lblDClass")
    event_date = label(doc, "lblDEventDate")
    for row in table_rows(doc, "grdSh_AEvt"):
        cells = h.cells_to_str(row)
        shares = cells.pop("total_number_of_shares")
        if cells.pop("column_0") != "Long position" or shares in (None, "0"):
            continue
        holding = make_ownership(
            context, filer, issuer_entity, shares, cells.pop("percentage_figure")
        )
        holding.add("sharesType", share_class)
        h.apply_date(holding, "date", event_date)
        holding.add("recordId", serial)
        holding.add("sourceUrl", url)
        context.emit(holding)
        context.audit_data(cells)

    crawl_controlled(
        context, doc, issuer_entity, filer, names, share_class, event_date, url
    )
    crawl_family(context, doc, filer, url)
    return filer


def crawl_issuer(
    context: Context, issuer: Issuer, listings: list[Listing] | None
) -> None:
    entity = context.make("Company")
    entity.id = context.make_slug(issuer.sid)
    entity.add("name", issuer.name)
    entity.add("topics", "corp.public")
    entity.add("sourceUrl", issuer.classes[0].shareholders_url)
    context.emit(entity)

    if listings is not None:
        primary = listings[0]
        if primary.isin is not None:
            security = h.make_security(context, primary.isin)
            security.add("issuer", entity)
            security.add("name", primary.name)
            for listing in listings:
                security.add("currency", listing.currency)
            context.emit(security)

    crawled: dict[str, Entity] = {}
    shareholders = [
        row
        for share_class in issuer.classes
        for row in list_rows(context, share_class.shareholders_url)
    ]
    for row in shareholders:
        name = h.element_text(row.pop("name_of_substantial_shareholder"))
        link = form_link(row.pop("form_serial_number"))
        if link is None:
            context.log.warning("Shareholder without form", name=name)
            continue
        kind, serial = link
        if kind not in ("NSForm1", "NSForm2", "NSForm3A"):
            context.log.warning("Unknown shareholder form", kind=kind, name=name)
            continue
        if serial not in crawled:
            crawled[serial] = crawl_form(context, issuer, entity, kind, serial, name)
        # The holding as at the last notice, which the form itself states.
        context.audit_data(
            h.cells_to_str(row),
            ignore=[
                "number_of_shares_interested_see_notes_above",
                "of_issued_voting_shares_see_notes_above",
                "date_of_last_notice_filed_dd_mm_yyyy",
            ],
        )

    directors = [
        (share_class.directors_url, row)
        for share_class in issuer.classes
        for row in list_rows(context, share_class.directors_url)
    ]
    for directors_url, row in directors:
        name = h.element_text(row.pop("name_of_director"))
        last_notice = h.element_text(row.pop("date_of_last_notice_filed_dd_mm_yyyy"))
        link = form_link(row.pop("form_serial_number"))
        if link is not None and link[0] == "NSForm3A":
            serial = link[1]
            if serial not in crawled:
                crawled[serial] = crawl_form(
                    context, issuer, entity, "NSForm3A", serial, name
                )
            director = crawled[serial]
        else:
            # Directors with interests in associated corporations only (Form 3B).
            if link is not None:
                context.log.warning("Unknown director form", kind=link[0], name=name)
            director = context.make("Person")
            director.id = context.make_id("person", issuer.sid, name)
            director.add("name", name)
            director.add("sourceUrl", directors_url)
            context.emit(director)
        directorship = context.make("Directorship")
        directorship.id = context.make_id("directorship", entity.id, director.id)
        directorship.add("organization", entity)
        directorship.add("director", director)
        directorship.add("role", "Director")
        h.apply_date(directorship, "date", last_notice or None)
        directorship.add("sourceUrl", directors_url)
        context.emit(directorship)
        # Holdings are read from the form; Form 3B (interests in associated
        # corporations) and debentures are not covered.
        context.audit_data(
            h.cells_to_str(row),
            ignore=[
                "no_of_shares_interested",
                "of_issued_voting_shares",
                "interests_in_shares_of_associated_corporation",
                "interests_in_debentures",
            ],
        )


def crawl(context: Context) -> None:
    assert context.data_url is not None
    path = context.fetch_resource("ListOfSecurities.xlsx", context.data_url)
    seen: set[str] = set()
    for listings in read_listings(context, path):
        code = listings[0].code
        issuers = search_issuers(context, code)
        if len(issuers) == 0:
            context.log.info("No disclosure records", code=code)
            continue
        if len(issuers) > 1:
            context.log.warning("Several issuers for one stock code", code=code)
        for issuer in issuers:
            if issuer.sid in seen:
                continue
            seen.add(issuer.sid)
            context.log.info("Crawling issuer", code=code, sid=issuer.sid)
            crawl_issuer(context, issuer, listings if len(issuers) == 1 else None)
    assert_all_accepted(context, raise_on_unaccepted=False)
