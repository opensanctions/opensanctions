import csv
import re
from collections import Counter
from contextlib import contextmanager
from io import TextIOWrapper
from pathlib import Path
from typing import IO
from collections.abc import Generator
from urllib.parse import urljoin
from zipfile import ZipFile

from followthemoney.types import registry
from lxml import etree
from normality import slugify

from zavod import Context, Entity
from zavod import helpers as h
from zavod.util import Element

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"  # noqa
LEI = "http://www.gleif.org/data/schema/leidata/2016"
RR = "http://www.gleif.org/data/schema/rr/2016"

CAT_URL = "https://www.gleif.org/en/lei-data/gleif-concatenated-file/download-the-concatenated-file"
BIC_URL = "https://mapping.gleif.org/api/v2/bic-lei/latest/download"
ISIN_URL = "https://mapping.gleif.org/api/v2/isin-lei/latest/download"
OC_URL = "https://mapping.gleif.org/api/v2/oc-lei/latest/download"

BR_TAG = re.compile(r"\s*,?\s*<br\s*/?>\s*", re.IGNORECASE)

RELATIONSHIPS: dict[str, tuple[str, str, str]] = {
    "IS_FUND-MANAGED_BY": ("Directorship", "organization", "director"),
    "IS_SUBFUND_OF": ("Directorship", "organization", "director"),
    "IS_DIRECTLY_CONSOLIDATED_BY": ("Ownership", "asset", "owner"),
    "IS_ULTIMATELY_CONSOLIDATED_BY": ("Ownership", "asset", "owner"),
    "IS_INTERNATIONAL_BRANCH_OF": ("Ownership", "asset", "owner"),
    "IS_FEEDER_TO": ("UnknownLink", "subject", "object"),
}

# Recognised enum values; anything else is logged as a warning so schema drift shows up.
ENTITY_STATUSES = {"ACTIVE", "INACTIVE", "NULL"}
LEI_REGISTRATION_STATUSES = {
    "ISSUED",
    "LAPSED",
    "RETIRED",
    "DUPLICATE",
    "ANNULLED",
    "PENDING_ARCHIVAL",
    "PENDING_TRANSFER",
}
# NULL marks relationship records whose registration was annulled (filed in error).
RELATIONSHIP_STATUSES = {"ACTIVE", "INACTIVE", "NULL"}
# LAPSED relationships are kept: the lapse follows the child LEI not being renewed,
# not the relationship ending.
RR_REGISTRATION_STATUSES = {
    "PUBLISHED",
    "LAPSED",
    "RETIRED",
    "DUPLICATE",
    "ANNULLED",
    "PENDING_ARCHIVAL",
    "PENDING_TRANSFER",
}
RR_DEAD_REGISTRATIONS = {"RETIRED", "DUPLICATE", "ANNULLED"}
PERIOD_TYPES = {"RELATIONSHIP_PERIOD", "ACCOUNTING_PERIOD", "DOCUMENT_FILING_PERIOD"}


ADDRESS_PARTS: dict[str, str] = {
    # map paths to zavod.parse.format_address input
    "City": "city",
    "Region": "state",
    "Country": "country_code",
    "PostalCode": "postal_code",
    "FirstAddressLine": "street",
    "AdditionalAddressLine": "street2",
}


def clean_text(text: str | None) -> str | None:
    """Turn markup-like source text into plain text.

    Angle brackets are used as quotes or room numbers ("<one> GmbH", "8号<8-6>室"),
    so they become look-alike quotation marks instead of being stripped.
    """
    if text is None:
        return None
    text = BR_TAG.sub(", ", text)
    return text.replace("<", "‹").replace(">", "›")


def parse_address(entity: Entity, el: Element | None) -> None:
    if el is None:
        return
    parts: dict[str, str | None] = {}  # FirstAddressLine
    for tag, key in ADDRESS_PARTS.items():
        parts[key] = clean_text(el.findtext(tag))
    country_code = parts.get("country_code")
    state = parts.get("state")
    if state is not None and country_code is not None:
        prefix = f"{country_code}-"
        if state.startswith(prefix):
            parts["state"] = state[len(prefix) :]
        if state == country_code:
            parts["state"] = None
    address = h.format_address(**parts)
    if address is not None and len(address) > registry.address.max_length:
        # Some street lines run to a paragraph of directions; keep the locality.
        address = h.format_address(**{**parts, "street": None, "street2": None})
    if address is not None:
        entity.add("address", address, lang=el.get("lang"))


def load_elfs() -> dict[str, str]:
    names = {}
    # https://www.gleif.org/en/about-lei/code-lists/iso-20275-entity-legal-forms-code-list#
    elf_path = Path(__file__).parent / "ref" / "elf-codes-1.4.1.csv"
    with open(elf_path) as fh:
        for row in csv.DictReader(fh):
            data = {slugify(k, sep="_"): v for k, v in row.items()}
            label = data["entity_legal_form_name_local_name"].strip()
            if len(label):
                names[data["elf_code"]] = label

    return names


def lei_id(lei: str) -> str:
    return f"lei-{lei}"


def parse_date(text: str | None) -> str | None:
    if text is None:
        return None
    return text.split("T")[0]


def fetch_cat_file(context: Context, url_part: str, name: str) -> Path | None:
    doc = context.fetch_html(CAT_URL)
    for link in doc.findall(".//a"):
        url = urljoin(CAT_URL, link.get("href"))
        if url_part in url:
            return context.fetch_resource(name, url)
    context.log.info("Failed HTML", url=CAT_URL)
    return None


def fetch_lei_file(context: Context) -> Path:
    path = fetch_cat_file(context, "/concatenated-files/lei2/get/", "lei.zip")
    if path is None:
        raise RuntimeError("Cannot find cat LEI2 file!")
    return path


def fetch_rr_file(context: Context) -> Path:
    path = fetch_cat_file(context, "/concatenated-files/rr/get/", "rr.zip")
    if path is None:
        raise RuntimeError("Cannot find cat RR file!")
    return path


@contextmanager
def read_zip_file(context: Context, path: Path) -> Generator[IO[bytes], None, None]:
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            context.log.info(f"Reading: {name} in {path}")
            with zip.open(name, "r") as fh:
                yield fh


def load_bic_mapping(context: Context) -> dict[str, list[str]]:
    zip_path = context.fetch_resource("bic_lei.zip", BIC_URL)
    mapping: dict[str, list[str]] = {}
    with read_zip_file(context, zip_path) as fh:
        textfh = TextIOWrapper(fh, encoding="utf-8")
        for row in csv.DictReader(textfh):
            lei = row.get("LEI")
            if lei is None:
                raise RuntimeError("No LEI in BIC/LEI mapping")
            mapping.setdefault(lei, [])
            bic = row.get("BIC")
            if bic is not None:
                mapping[lei].append(bic)
    return mapping


def load_oc_mapping(context: Context) -> dict[str, list[str]]:
    zip_path = context.fetch_resource("oc_lei.zip", OC_URL)
    mapping: dict[str, list[str]] = {}
    with read_zip_file(context, zip_path) as fh:
        textfh = TextIOWrapper(fh, encoding="utf-8")
        for row in csv.DictReader(textfh):
            lei = row.get("LEI")
            if lei is None:
                raise RuntimeError("No LEI in BIC/LEI mapping")
            mapping.setdefault(lei, [])
            oc_id = row.get("OpenCorporatesID")
            if oc_id is not None:
                oc_url = f"https://opencorporates.com/companies/{oc_id}"
                mapping[lei].append(oc_url)
    return mapping


def load_isin_mapping(context: Context) -> dict[str, list[str]]:
    zip_path = context.fetch_resource("isin_lei.zip", ISIN_URL)
    mapping: dict[str, list[str]] = {}
    with read_zip_file(context, zip_path) as fh:
        textfh = TextIOWrapper(fh, encoding="utf-8")
        for row in csv.DictReader(textfh):
            lei = row.get("LEI")
            if lei is None:
                raise RuntimeError("No LEI in BIC/LEI mapping")
            mapping.setdefault(lei, [])
            isin = row.get("ISIN")
            if isin is not None:
                mapping[lei].append(isin)
    return mapping


def parse_lei_record(
    context: Context,
    el: Element,
    *,
    bics: dict[str, list[str]],
    ocurls: dict[str, list[str]],
    isins: dict[str, list[str]],
    elfs: dict[str, str],
    skipped: set[str],
) -> None:
    elc = h.remove_namespace(el)
    lei = elc.findtext("LEI")
    if lei is None:
        return
    registration = elc.find("Registration")
    if registration is not None:
        reg_status = registration.findtext("RegistrationStatus")
        if reg_status not in LEI_REGISTRATION_STATUSES:
            context.log.warning(
                "Unknown LEI RegistrationStatus", status=reg_status, lei=lei
            )
        # Issued in error or superseded by another LEI; often fail the LEI checksum.
        if reg_status in ("ANNULLED", "DUPLICATE"):
            skipped.add(lei)
            return
    proxy = context.make("Organization")
    proxy.id = lei_id(lei)
    entity = elc.find("Entity")
    if entity is None:
        return
    name = entity.find("LegalName")
    if name is not None:
        proxy.add("name", clean_text(name.text), lang=name.get("lang"))
    proxy.add("jurisdiction", entity.findtext("LegalJurisdiction"))

    # INACTIVE and NULL entities are still emitted because they may be owners of others.
    status = entity.findtext("EntityStatus")
    if status not in ENTITY_STATUSES:
        context.log.warning("Unknown EntityStatus", status=status, lei=lei)
    if status == "INACTIVE":
        proxy.add("status", status)

    create_date = parse_date(entity.findtext("EntityCreationDate"))
    proxy.add("incorporationDate", create_date)
    authority = entity.find("RegistrationAuthority")
    if authority is not None:
        reg_id = authority.findtext("RegistrationAuthorityEntityID")
        # Some registers give a prose citation ("Tomo 5, Folio 77, Hoja M-6").
        if reg_id is not None and len(reg_id) <= registry.identifier.max_length:
            proxy.add("registrationNumber", reg_id)

    proxy.add_cast("Company", "swiftBic", bics.get(lei))
    proxy.add("leiCode", lei, quiet=True)
    proxy.add_cast("Company", "opencorporatesUrl", ocurls.get(lei))
    # proxy.add("sourceUrl", f"https://search.gleif.org/#/record/{lei}")

    for isin in isins.get(lei, []):
        proxy.add_schema("Company")
        proxy.add("topics", "corp.public")
        security = h.make_security(context, isin)
        security.add("issuer", proxy.id)
        security.add("country", entity.findtext("LegalJurisdiction"))
        context.emit(security)

    legal_form = entity.find("LegalForm")
    if legal_form is not None:
        code = legal_form.findtext("EntityLegalFormCode")
        if code is not None:
            proxy.add("legalForm", elfs.get(code))
        proxy.add("legalForm", legal_form.findtext("OtherLegalForm"))

    if registration is not None:
        mod_date = parse_date(registration.findtext("LastUpdateDate"))
        proxy.add("modifiedAt", mod_date)

    successor = elc.find("SuccessorEntity")
    if successor is not None:
        succ_lei = successor.findtext("SuccessorLEI")
        if succ_lei is None:
            return
        succession = context.make("Succession")
        succession.id = f"lei-succession-{lei}-{succ_lei}"
        succession.add("predecessor", lei)
        succession.add("successor", lei_id(succ_lei))
        context.emit(succession)

    parse_address(proxy, entity.find("LegalAddress"))
    parse_address(proxy, entity.find("HeadquartersAddress"))

    context.emit(proxy)


def parse_lei_file(context: Context, fh: IO[bytes]) -> set[str]:
    """Emit all LEI records and return the LEIs that were skipped as invalid."""
    elfs = load_elfs()
    bics = load_bic_mapping(context)
    ocurls = load_oc_mapping(context)
    isins = load_isin_mapping(context)
    skipped: set[str] = set()

    idx = 0
    for idx, (_, el) in enumerate(etree.iterparse(fh, tag=f"{{{LEI}}}LEIRecord")):
        if idx > 0 and idx % 10000 == 0:
            context.log.info(f"Parse LEIRecord: {idx}...")
        parse_lei_record(
            context,
            el,
            bics=bics,
            ocurls=ocurls,
            isins=isins,
            elfs=elfs,
            skipped=skipped,
        )
        el.clear()

    if idx == 0:
        raise RuntimeError("No entities!")
    return skipped


def parse_relationship_record(
    context: Context,
    el: etree._Element,
    skipped: set[str],
    dropped: Counter[tuple[str | None, str | None]],
) -> None:
    elc = h.remove_namespace(el)
    rel = elc.find("Relationship")
    if rel is None:
        return

    status = rel.findtext("RelationshipStatus")
    reg_status = elc.findtext("Registration/RegistrationStatus")
    if status not in RELATIONSHIP_STATUSES:
        context.log.warning("Unknown RelationshipStatus", status=status)
    if reg_status not in RR_REGISTRATION_STATUSES:
        context.log.warning("Unknown RR RegistrationStatus", status=reg_status)
    if status != "ACTIVE":
        dropped[(status, reg_status)] += 1
        return
    if reg_status in RR_DEAD_REGISTRATIONS:
        context.log.warning(
            "Active relationship with dead registration", registration=reg_status
        )
        dropped[(status, reg_status)] += 1
        return

    rel_type = rel.findtext("RelationshipType")
    start_node = rel.find("StartNode")
    end_node = rel.find("EndNode")
    if rel_type is None or start_node is None or end_node is None:
        return
    rel_data = RELATIONSHIPS.get(rel_type)
    if rel_data is None:
        context.log.warn("Unknown relationship: %s", rel_type)
        return
    rel_schema, start_prop, end_prop = rel_data

    start_node_type = start_node.findtext("NodeIDType")
    if start_node_type != "LEI":
        context.log.warn("Unknown edge type", node_id_type=start_node_type)
        return
    start_lei = start_node.findtext("NodeID")

    end_node_type = end_node.findtext("NodeIDType")
    if end_node_type != "LEI":
        context.log.warn("Unknown edge type", node_id_type=end_node_type)
        return
    end_lei = end_node.findtext("NodeID")

    if start_lei is None or end_lei is None:
        context.log.warn("Relationship missing LEI", start=start_lei, end=end_lei)
        return
    if start_lei in skipped or end_lei in skipped:
        return

    proxy = context.make(rel_schema)
    rel_id = slugify(rel_type, sep="-")
    proxy.id = f"lei-{start_lei}-{rel_id}-{end_lei}"
    proxy.add(start_prop, lei_id(start_lei))
    proxy.add(end_prop, lei_id(end_lei))
    proxy.add("role", rel_type.replace("_", " "))
    proxy.add("status", status)

    if rel_schema == "Ownership":
        # Organization entities cannot be assets, so we re-emit the asset entity as a Company
        assert start_prop == "asset"
        asset = context.make("Company")
        asset.id = lei_id(start_lei)
        # Also emit leiCode so that Context.emit doesn't complain about the Entity having no properties
        asset.add("leiCode", start_lei)
        context.emit(asset)

    for period in rel.findall(".//RelationshipPeriod"):
        period_type = period.findtext("PeriodType")
        if period_type not in PERIOD_TYPES:
            context.log.warning("Unknown PeriodType", period_type=period_type)
        if period_type == "RELATIONSHIP_PERIOD":
            proxy.add("startDate", parse_date(period.findtext("StartDate")))
            proxy.add("endDate", parse_date(period.findtext("EndDate")))

    for quant in rel.findall(".//RelationshipQuantifier"):
        amount = quant.findtext("QuantifierAmount")
        units = quant.findtext("QuantifierUnits")
        if units == "PERCENTAGE" or units is None:
            proxy.add("percentage", amount, quiet=True)
        else:
            context.log.warn("Unknown rel quantifier", amount=amount, units=units)

    context.emit(proxy)


def parse_rr_file(context: Context, fh: IO[bytes], skipped: set[str]) -> None:
    tag = f"{{{RR}}}RelationshipRecord"
    dropped: Counter[tuple[str | None, str | None]] = Counter()
    idx = 0
    for idx, (_, el) in enumerate(etree.iterparse(fh, tag=tag)):
        if idx > 0 and idx % 10000 == 0:
            context.log.info(f"Parse RelationshipRecord: {idx}...")
        parse_relationship_record(context, el, skipped, dropped)
        el.clear()

    if idx == 0:
        raise RuntimeError("No relationships!")
    for (status, reg_status), count in dropped.most_common():
        context.log.info(
            "Dropped non-active relationships",
            status=status,
            registration=reg_status,
            count=count,
        )


def crawl(context: Context) -> None:
    lei_file = fetch_lei_file(context)
    rr_file = fetch_rr_file(context)
    with read_zip_file(context, lei_file) as fh:
        skipped = parse_lei_file(context, fh)
    with read_zip_file(context, rr_file) as fh:
        parse_rr_file(context, fh, skipped)
