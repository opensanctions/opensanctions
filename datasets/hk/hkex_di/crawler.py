import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

from followthemoney import registry
from lxml.etree import _Element as Element
from normality import normalize
from python_calamine import CalamineWorkbook

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
    notices_url: str | None


@dataclass
class Issuer:
    sid: str
    # Every name the issuer was searched under: a rename shows as another row.
    names: list[str]
    classes: list[ShareClass]


class Ownerships:
    """Keep each ownership link from its most recent notice, since several
    notices can report the same link as at different dates."""

    def __init__(self) -> None:
        self.latest: dict[str, tuple[str, Entity]] = {}

    def add(self, ownership: Entity) -> None:
        assert ownership.id is not None
        date = max(ownership.get("date"), default="")
        known = self.latest.get(ownership.id)
        if known is None or date > known[0]:
            self.latest[ownership.id] = (date, ownership)

    def emit(self, context: Context) -> None:
        for _, ownership in self.latest.values():
            context.emit(ownership)


def fetch_page(
    context: Context, url: str, marker: str, cache_days: int | None = None
) -> Element:
    """Fetch a page, retrying with growing pauses while HKEX serves its error
    page in its place. The marker is the ID of a label that only real pages
    carry, and fill in: HKEX also serves pages with the labels left blank."""
    for attempt in range(5):
        if attempt > 0:
            time.sleep(10 * 2 ** (attempt - 1))
        doc = context.fetch_html(url, cache_days=cache_days, absolute_links=True)
        markers = h.xpath_elements(doc, f'//span[@id="{marker}"]')
        if len(markers) == 1 and h.element_text(markers[0]) != "":
            return doc
        context.clear_url(url)
        context.log.info("Error page, retrying", url=url, attempt=attempt)
    raise ValueError(f"HKEX error page: {url}")


def label(doc: Element, id: str) -> str | None:
    spans = h.xpath_elements(doc, f'//span[@id="{id}"]')
    if len(spans) == 0:
        return None
    return h.element_text(spans[0]) or None


def is_blank(cells: list[Element]) -> bool:
    """Tell whether a row is empty, or holds only the "Nil" some filers enter."""
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
    """Read a table with tables nested in its cells, which `h.parse_html_table`
    would read as rows of the outer table."""
    rows: list[list[Element]] = []
    for tr in h.xpath_elements(doc, f'//table[@id="{id}"]/tr')[1:]:
        cells = tr.findall("./td")
        if not is_blank(cells):
            rows.append(cells)
    return rows


def form_link(cell: Element) -> tuple[str, str] | None:
    """Return the form page (e.g. "NSForm2") and the link of a list cell."""
    links = h.xpath_elements(cell, ".//a[@href]")
    if len(links) == 0:
        return None
    href = links[0].get("href", "")
    return urlparse(href).path.rsplit("/", 1)[-1].removesuffix(".aspx"), href


def split_resigned(name: str | None) -> tuple[str | None, str | None]:
    """Split off the "(Resigned on 01/10/2016)" HKEX appends to a director's name."""
    if name is None:
        return None, None
    match = re.match(r"^(.*?)\s*\(Resigned on ([\d/]+)\)$", name)
    if match is None:
        return name, None
    return match.group(1) or None, match.group(2)


def name_key(context: Context, name: str | None) -> str | None:
    """Key a name so that "Mr. Bernard Chan Charnwut" and "Chan Bernard
    Charnwut" match."""
    norm = normalize(h.strip_name_titles(context, name), lowercase=True, ascii=True)
    if norm is None:
        return None
    return " ".join(sorted(norm.split()))


def reference_names(name: str) -> list[str]:
    """Return the names other rows of a form may use for a company: without a
    defined term ("Henderson Development Limited ("HD")") or former name, and
    the defined term itself."""
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
    """Split the place of incorporation off an address: "..., Grand Cayman
    (Cayman Islands)"."""
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
        if row.pop("category") != "Equity":
            continue
        code = row.pop("stock_code")
        name = row.pop("name_of_securities")
        assert code is not None and name is not None, row
        isin = row.pop("isin")
        currency = row.pop("trading_currency")
        listing = Listing(code=code, name=name, isin=isin, currency=currency)
        groups.setdefault(isin or code, []).append(listing)
        context.audit_data(
            row,
            ignore=[
                "sub_category",
                "board_lot",
                "expiry_date",
                "subject_to_stamp_duty",
                "shortsell_eligible",
                "cas_eligible",
                "vcm_eligible",
                "admitted_to_ccass",
                "debt_securities_board_lot_nominal",
                "debt_securities_investor_type",
                "pos_eligible",
                "spread_table1_part_a3_part_b5_part_d4_6_part_e",
                # The RMB counter is listed in a row of its own.
                "rmb_counter",
            ],
        )
    for group in groups.values():
        group.sort(key=lambda listing: listing.currency != "HKD")
    return list(groups.values())


def search_issuers(context: Context, base_url: str, code: str) -> list[Issuer]:
    """Search the issuers that filed under a stock code, in the electronic
    filing system or in the archive of notices filed before it (`base_url`)."""
    # Part XV of the Securities and Futures Ordinance took effect on 1 April 2003.
    end_date = settings.RUN_TIME.strftime("%d/%m/%Y")
    url = urljoin(
        base_url,
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
        notices_url = links.get("list_of_notices_filed_by_substantial_shareholders")
        rows.setdefault(sid, []).append(
            (name, ShareClass(shareholders_url, directors_url, notices_url))
        )
    issuers: list[Issuer] = []
    for sid, classes in rows.items():
        names: list[str] = []
        for name, _ in classes:
            name = name.rsplit(" - ", 1)[0].strip() if len(classes) > 1 else name
            if name not in names:
                names.append(name)
        issuers.append(Issuer(sid=sid, names=names, classes=[c for _, c in classes]))
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
    share_class: str | None = None,
) -> Entity:
    """Make an ownership link, one per share class for a holding in a listed
    company, since its percentage is of the issued shares in that class."""
    ownership = context.make("Ownership")
    ownership.id = context.make_id("ownership", owner.id, asset.id, share_class)
    ownership.add("owner", owner)
    ownership.add("asset", asset)
    if shares is not None:
        # sharesCount is a string property; store the count without separators.
        count, unit = registry.number.parse(shares)
        if count is None or unit is not None:
            context.log.warning("Cannot parse share count", value=shares)
        ownership.add("sharesCount", count)
    if percentage is not None and not re.fullmatch(r"\d+(\.\d+)?", percentage):
        context.log.warning("Cannot parse percentage", value=percentage)
    elif percentage is not None and float(percentage) > 100:
        # Interests counting derivatives, such as convertible notes, can
        # exceed the issued shares.
        context.log.info("Percentage over 100", owner=owner.id, value=percentage)
    else:
        ownership.add("percentage", percentage)
    return ownership


def make_filer(
    context: Context, doc: Element, issuer: Issuer, kind: str, list_name: str
) -> tuple[Entity, list[str]]:
    """Make the filer of a form, with the names other rows of the form use for it."""
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
    first, _ = split_resigned(label(doc, "lblDFirstname"))
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
    ownerships: Ownerships,
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
        corp.id = context.make_id("company", filer.id, raw_name, place)
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
        long_shares = re.search(r"Long position\s*([\d,]+)", shares, re.I)
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
            parent = resolve_controller(
                context, known, party, filer, issuer_entity, url
            )
            if parent is None or parent.id == corp.id:
                continue
            joint = percent if len(parties) == 1 else None
            link = make_ownership(context, parent, corp, None, joint)
            h.apply_date(link, "date", event_date)
            link.add("sourceUrl", url)
            ownerships.add(link)
        if is_direct and held is not None:
            holding = make_ownership(
                context, corp, issuer_entity, held, None, share_class
            )
            holding.add("sharesType", share_class)
            h.apply_date(holding, "date", event_date)
            holding.add("sourceUrl", url)
            ownerships.add(holding)


def resolve_controller(
    context: Context,
    known: dict[str, Entity],
    controller: str,
    filer: Entity,
    issuer_entity: Entity,
    url: str,
) -> Entity | None:
    """Find the entity a form row names as controlling person, or make one for
    a controller no row of the form describes, such as a family trust's trustee."""
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
    parent.id = context.make_id("controller", filer.id, controller)
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
        if len(cells) == 2:
            # Forms filed before 2004 or so name the child or spouse without
            # saying which they are.
            context.log.info("Family member without relation", url=url)
            continue
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
    link: str,
    serial: str,
    list_name: str,
    ownerships: Ownerships,
) -> Entity:
    """Crawl a notice form, given the form link of a list row."""
    fn = parse_qs(urlparse(link).query)["fn"][0]
    url = urljoin(link, f"{kind}.aspx?fn={fn}")
    # Forms in the archive of notices filed before the electronic filing system
    # label their serial differently, and are linked by a number.
    serial_label = "lblDLogNo" if "/filing/di/" in url else "lblDSerialNo"
    # A form never changes: an amendment is filed as a new form with its own
    # serial, which appends "(Amendment to <serial>)" to the form's serial label.
    doc = fetch_page(context, url, serial_label, cache_days=3650)
    form_serial = label(doc, serial_label)
    if form_serial is None or not form_serial.startswith(serial):
        context.clear_url(url)
        raise ValueError(f"Form serial {form_serial!r} is not {serial!r}: {url}")
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
        position = cells.pop("column_0") or ""
        if position.lower() != "long position" or shares in (None, "0"):
            continue
        holding = make_ownership(
            context,
            filer,
            issuer_entity,
            shares,
            cells.pop("percentage_figure"),
            share_class,
        )
        holding.add("sharesType", share_class)
        h.apply_date(holding, "date", event_date)
        holding.add("recordId", serial)
        holding.add("sourceUrl", url)
        ownerships.add(holding)
        context.audit_data(cells)

    crawl_controlled(
        context,
        doc,
        issuer_entity,
        filer,
        names,
        share_class,
        event_date,
        url,
        ownerships,
    )
    crawl_family(context, doc, filer, url)
    return filer


def crawl_shareholder(
    context: Context,
    issuer: Issuer,
    entity: Entity,
    row: dict[str, Element],
    crawled: dict[str, Entity],
    ownerships: Ownerships,
) -> None:
    # The archive of notices filed before the electronic filing system heads
    # its columns "Form Serial No" and "% of issued share capital".
    serial_cell = row.pop("form_serial_number", None)
    if serial_cell is None:
        serial_cell = row.pop("form_serial_no")
    name = h.element_text(row.pop("name_of_substantial_shareholder"))
    serial = h.element_text(serial_cell)
    link = form_link(serial_cell)
    if link is None:
        context.log.warning("Shareholder without form", name=name)
        return
    kind, href = link
    if kind not in ("NSForm1", "NSForm2", "NSForm3A"):
        context.log.warning("Unknown shareholder form", kind=kind, name=name)
        return
    if serial not in crawled:
        crawled[serial] = crawl_form(
            context, issuer, entity, kind, href, serial, name, ownerships
        )
    # The holding as at the last notice, which the form itself states.
    context.audit_data(
        h.cells_to_str(row),
        ignore=[
            "number_of_shares_interested_see_notes_above",
            "of_issued_voting_shares_see_notes_above",
            "of_issued_share_capital_see_notes_above",
            "date_of_last_notice_filed_dd_mm_yyyy",
        ],
    )


def notice_date(row: dict[str, Element]) -> str:
    """Return the date of the last notice of a list row as YYYY-MM-DD."""
    text = h.element_text(row["date_of_last_notice_filed_dd_mm_yyyy"])
    day, month, year = text.split("/")
    return f"{year}-{month}-{day}"


def notice_filers(context: Context, issuer: Issuer) -> set[str]:
    """Return the name keys of everyone who filed a substantial shareholder
    notice for the issuer through the electronic filing system."""
    filers: set[str] = set()
    for share_class in issuer.classes:
        if share_class.notices_url is None:
            continue
        for row in list_rows(context, share_class.notices_url):
            name = h.element_text(row["name_of_substantial_shareholder"])
            key = name_key(context, name)
            if key is not None:
                filers.add(key)
    return filers


def crawl_archived_shareholders(
    context: Context,
    issuer: Issuer,
    archived: Issuer,
    entity: Entity,
    current: set[str],
    crawled: dict[str, Entity],
    ownerships: Ownerships,
) -> None:
    """Crawl the substantial shareholders whose last notice predates the
    electronic filing system, unless they filed through it since: a holder that
    fell below 5% filed a notice but is no longer listed."""
    # The archive can list a holder twice, with notices years apart: keep the
    # most recent.
    latest: dict[str, dict[str, Element]] = {}
    for share_class in archived.classes:
        for row in list_rows(context, share_class.shareholders_url):
            name = h.element_text(row["name_of_substantial_shareholder"])
            key = name_key(context, name)
            if key is None or key in current:
                continue
            if key not in latest or notice_date(row) > notice_date(latest[key]):
                latest[key] = row
    if len(latest) == 0:
        return
    filers = notice_filers(context, issuer)
    for key, row in latest.items():
        if key not in filers:
            crawl_shareholder(context, issuer, entity, row, crawled, ownerships)


def crawl_issuer(
    context: Context,
    issuer: Issuer,
    archived: Issuer | None,
    listings: list[Listing] | None,
    ownerships: Ownerships,
) -> None:
    entity = context.make("Company")
    entity.id = context.make_slug(issuer.sid)
    entity.add("name", issuer.names[0])
    if archived is not None:
        current_name = name_key(context, issuer.names[0])
        for name in archived.names:
            if name_key(context, name) != current_name:
                entity.add("previousName", name)
    entity.add("topics", "corp.public")
    entity.add("sourceUrl", issuer.classes[0].shareholders_url)
    for listing in listings or []:
        entity.add("ticker", listing.code)
    context.emit(entity)

    if listings is not None:
        primary = listings[0]
        if primary.isin is not None:
            security = h.make_security(context, primary.isin)
            security.add("issuer", entity)
            security.add("name", primary.name)
            for listing in listings:
                security.add("ticker", listing.code)
                security.add("currency", listing.currency)
            context.emit(security)

    crawled: dict[str, Entity] = {}
    current: set[str] = set()
    for share_class in issuer.classes:
        for row in list_rows(context, share_class.shareholders_url):
            key = name_key(
                context, h.element_text(row["name_of_substantial_shareholder"])
            )
            if key is not None:
                current.add(key)
            crawl_shareholder(context, issuer, entity, row, crawled, ownerships)
    if archived is not None:
        crawl_archived_shareholders(
            context, issuer, archived, entity, current, crawled, ownerships
        )

    directors = [
        (share_class.directors_url, row)
        for share_class in issuer.classes
        for row in list_rows(context, share_class.directors_url)
    ]
    for directors_url, row in directors:
        name = h.element_text(row.pop("name_of_director"))
        last_notice = h.element_text(row.pop("date_of_last_notice_filed_dd_mm_yyyy"))
        serial_cell = row.pop("form_serial_number")
        link = form_link(serial_cell)
        if link is not None and link[0] == "NSForm3A":
            serial = h.element_text(serial_cell)
            if serial not in crawled:
                crawled[serial] = crawl_form(
                    context,
                    issuer,
                    entity,
                    "NSForm3A",
                    link[1],
                    serial,
                    name,
                    ownerships,
                )
            director = crawled[serial]
        else:
            # A Form 3B covers interests in associated corporations only, so it
            # holds no interest in the issuer.
            if link is not None and link[0] != "NSForm3B":
                context.log.warning("Unknown director form", kind=link[0], name=name)
            director = context.make("Person")
            director.id = context.make_id("person", issuer.sid, name)
            director.add("name", split_resigned(name)[0])
            director.add("sourceUrl", directors_url)
            context.emit(director)
        directorship = context.make("Directorship")
        directorship.id = context.make_id("directorship", entity.id, director.id)
        directorship.add("organization", entity)
        directorship.add("director", director)
        directorship.add("role", "Director")
        h.apply_date(directorship, "date", last_notice or None)
        h.apply_date(directorship, "endDate", split_resigned(name)[1])
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


def match_archived(
    context: Context, code: str, issuer: Issuer, archived: list[Issuer]
) -> Issuer | None:
    """Find the archived issuer that is the same company: a stock code can have
    belonged to a company since delisted, so its names must match."""

    # An archived issuer with one share class can still carry its class:
    # "Industrial and Commercial Bank of China Ltd. - H Shares".
    def keys(names: list[str]) -> set[str]:
        found = {name_key(context, name.rsplit(" - ", 1)[0]) for name in names}
        return {key for key in found if key is not None}

    matches = [other for other in archived if keys(issuer.names) & keys(other.names)]
    if len(matches) > 1:
        context.log.warning("Several archived issuers match", code=code)
        return None
    if len(matches) == 0 and len(archived) > 0:
        context.log.info(
            "No archived issuer matches",
            code=code,
            names=issuer.names,
            archived=[name for other in archived for name in other.names],
        )
        return None
    return matches[0] if matches else None


def crawl(context: Context) -> None:
    assert context.data_url is not None
    assert context.dataset.url is not None
    # Notices filed before the electronic filing system opened on 3 July 2017
    # are kept in a separate archive with the same search and list pages.
    archive_url = urljoin(context.dataset.url, "/filing/di/")
    path = context.fetch_resource("ListOfSecurities.xlsx", context.data_url)
    seen: set[str] = set()
    ownerships = Ownerships()
    for listings in read_listings(context, path):
        code = listings[0].code
        issuers = search_issuers(context, context.dataset.url, code)
        if len(issuers) == 0:
            context.log.info("No disclosure records", code=code)
            continue
        if len(issuers) > 1:
            context.log.warning("Several issuers for one stock code", code=code)
        archived = search_issuers(context, archive_url, code)
        for issuer in issuers:
            if issuer.sid in seen:
                continue
            seen.add(issuer.sid)
            if len(issuer.names) > 1:
                context.log.warning("Inconsistent issuer names", names=issuer.names)
            context.log.info("Crawling issuer", code=code, sid=issuer.sid)
            crawl_issuer(
                context,
                issuer,
                match_archived(context, code, issuer, archived),
                listings if len(issuers) == 1 else None,
                ownerships,
            )
    ownerships.emit(context)
    assert_all_accepted(context, raise_on_unaccepted=False)
