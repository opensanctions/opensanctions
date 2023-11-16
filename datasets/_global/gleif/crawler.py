import csv
from contextlib import contextmanager
from io import TextIOWrapper
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin
from zipfile import ZipFile

from lxml import etree, html
from normality import slugify
from zavod import Context
from zavod import helpers as h

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"  # noqa
LEI = "http://www.gleif.org/data/schema/leidata/2016"
RR = "http://www.gleif.org/data/schema/rr/2016"

CAT_URL = "https://www.gleif.org/en/lei-data/gleif-concatenated-file/download-the-concatenated-file"
BIC_URL = "https://mapping.gleif.org/api/v2/bic-lei/latest/download"
ISIN_URL = "https://mapping.gleif.org/api/v2/isin-lei/latest/download"
OC_URL = "https://mapping.gleif.org/api/v2/oc-lei/latest/download"

RELATIONSHIPS: Dict[str, Tuple[str, str, str]] = {
    "IS_FUND-MANAGED_BY": ("Directorship", "organization", "director"),
    "IS_SUBFUND_OF": ("Directorship", "organization", "director"),
    "IS_DIRECTLY_CONSOLIDATED_BY": ("Ownership", "asset", "owner"),
    "IS_ULTIMATELY_CONSOLIDATED_BY": ("Ownership", "asset", "owner"),
    "IS_INTERNATIONAL_BRANCH_OF": ("Ownership", "asset", "owner"),
    "IS_FEEDER_TO": ("UnknownLink", "subject", "object"),
}


ADDRESS_PARTS: Dict[str, str] = {
    # map paths to zavod.parse.format_address input
    "City": "city",
    "Region": "state",
    "Country": "country_code",
    "PostalCode": "postal_code",
}


def make_address(el: etree._Element) -> str:
    parts: Dict[str, Union[str, None]] = {"summary": el.text}  # FirstAddressLine
    parent = el.getparent()
    if parent is None:
        return ""
    for tag, key in ADDRESS_PARTS.items():
        parts[key] = parent.findtext(tag)
    return h.format_address(**parts)


def load_elfs() -> Dict[str, str]:
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


def parse_date(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return text.split("T")[0]


def fetch_cat_file(context: Context, url_part: str, name: str) -> Optional[Path]:
    res = context.http.get(CAT_URL)
    doc = html.fromstring(res.text)
    for link in doc.findall(".//a"):
        url = urljoin(CAT_URL, link.get("href"))
        if url_part in url:
            return context.fetch_resource(name, url)
    context.log.info("Failed HTML", url=CAT_URL, html=res.text)
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
def read_zip_file(context: Context, path: Path):
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            context.log.info("Reading: %s in %s" % (name, path))
            with zip.open(name, "r") as fh:
                yield fh


def load_bic_mapping(context: Context) -> Dict[str, List[str]]:
    zip_path = context.fetch_resource("bic_lei.zip", BIC_URL)
    mapping: Dict[str, List[str]] = {}
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


def load_oc_mapping(context: Context) -> Dict[str, List[str]]:
    zip_path = context.fetch_resource("oc_lei.zip", OC_URL)
    mapping: Dict[str, List[str]] = {}
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


def load_isin_mapping(context: Context) -> Dict[str, List[str]]:
    zip_path = context.fetch_resource("isin_lei.zip", ISIN_URL)
    mapping: Dict[str, List[str]] = {}
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


def parse_lei_file(context: Context, fh: BinaryIO) -> None:
    elfs = load_elfs()
    bics = load_bic_mapping(context)
    ocurls = load_oc_mapping(context)
    isins = load_isin_mapping(context)
    for idx, (_, el) in enumerate(etree.iterparse(fh, tag="{%s}LEIRecord" % LEI)):
        if idx > 0 and idx % 10000 == 0:
            context.log.info("Parse LEIRecord: %d..." % idx)
        elc = h.remove_namespace(el)
        proxy = context.make("Organization")
        lei = elc.findtext("LEI")
        if lei is None:
            continue
        proxy.id = lei_id(lei)
        entity = elc.find("Entity")
        if entity is None:
            continue
        proxy.add("name", entity.findtext("LegalName"))
        proxy.add("jurisdiction", entity.findtext("LegalJurisdiction"))
        status = entity.findtext("EntityStatus")
        if status in ("DUPLICATE", "ANNULLED"):
            continue
        proxy.add("status", status)
        create_date = parse_date(entity.findtext("EntityCreationDate"))
        proxy.add("incorporationDate", create_date)
        authority = entity.find("RegistrationAuthority")
        if authority is not None:
            reg_id = authority.findtext("RegistrationAuthorityEntityID")
            proxy.add("registrationNumber", reg_id)

        proxy.add_cast("Company", "swiftBic", bics.get(lei))
        proxy.add("leiCode", lei, quiet=True)
        proxy.add_cast("Company", "opencorporatesUrl", ocurls.get(lei))

        for isin in isins.get(lei, []):
            proxy.add_schema("Company")
            proxy.add("topics", "corp.public")
            security = context.make("Security")
            security.id = f"lei-isin-{isin}"
            security.add("isin", isin)
            security.add("issuer", proxy.id)
            security.add("country", entity.findtext("LegalJurisdiction"))
            context.emit(security)

        legal_form = entity.find("LegalForm")
        if legal_form is not None:
            code = legal_form.findtext("EntityLegalFormCode")
            if code is not None:
                proxy.add("legalForm", elfs.get(code))
            proxy.add("legalForm", legal_form.findtext("OtherLegalForm"))

        registration = elc.find("Registration")
        if registration is not None:
            mod_date = parse_date(registration.findtext("LastUpdateDate"))
            proxy.add("modifiedAt", mod_date)

        # pprint(proxy.to_dict())

        successor = elc.find("SuccessorEntity")
        if successor is not None:
            succ_lei = successor.findtext("SuccessorLEI")
            if succ_lei is None:
                continue
            succession = context.make("Succession")
            succession.id = f"lei-succession-{lei}-{succ_lei}"
            succession.add("predecessor", lei)
            succession.add("successor", lei_id(succ_lei))
            context.emit(succession)

        for address_el in el.findall(".//FirstAddressLine"):
            proxy.add("address", make_address(address_el))

        el.clear()
        context.emit(proxy)

    if idx == 0:
        raise RuntimeError("No entities!")


def parse_rr_file(context: Context, fh: BinaryIO):
    tag = "{%s}RelationshipRecord" % RR
    for idx, (_, el) in enumerate(etree.iterparse(fh, tag=tag)):
        if idx > 0 and idx % 10000 == 0:
            context.log.info("Parse RelationshipRecord: %d..." % idx)
        elc = h.remove_namespace(el)
        # print(elc)
        rel = elc.find("Relationship")
        if rel is None:
            continue
        rel_type = rel.findtext("RelationshipType")
        start_node = rel.find("StartNode")
        end_node = rel.find("EndNode")
        if rel_type is None or start_node is None or end_node is None:
            continue
        rel_data = RELATIONSHIPS.get(rel_type)
        if rel_data is None:
            context.log.warn("Unknown relationship: %s", rel_type)
            continue
        rel_schema, start_prop, end_prop = rel_data

        start_node_type = start_node.findtext("NodeIDType")
        if start_node_type != "LEI":
            context.log.warn("Unknown edge type", node_id_type=start_node_type)
            continue
        start_lei = start_node.findtext("NodeID")

        end_node_type = end_node.findtext("NodeIDType")
        if end_node_type != "LEI":
            context.log.warn("Unknown edge type", node_id_type=end_node_type)
            continue
        end_lei = end_node.findtext("NodeID")

        if start_lei is None or end_lei is None:
            context.log.warn("Relationship missing LEI", start=start_lei, end=end_lei)
            continue

        proxy = context.make(rel_schema)
        rel_id = slugify(rel_type, sep="-")
        proxy.id = f"lei-{start_lei}-{rel_id}-{end_lei}"
        proxy.add(start_prop, lei_id(start_lei))
        proxy.add(end_prop, lei_id(end_lei))
        proxy.add("role", rel_type.replace("_", " "))
        proxy.add("status", rel.findtext("RelationshipStatus"))

        for period in rel.findall(".//RelationshipPeriod"):
            period_type = period.findtext("PeriodType")
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

        el.clear()
        context.emit(proxy)

    if idx == 0:
        raise RuntimeError("No relationships!")


def crawl(context: Context):
    lei_file = fetch_lei_file(context)
    rr_file = fetch_rr_file(context)
    with read_zip_file(context, lei_file) as fh:
        parse_lei_file(context, fh)
    with read_zip_file(context, rr_file) as fh:
        parse_rr_file(context, fh)
