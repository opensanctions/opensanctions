import csv
from io import TextIOWrapper
from typing import Generator, Dict
import zipfile
from rigour.mime.types import ZIP

from zavod import Context
from zavod import helpers as h

CSVIter = Generator[Dict[str, str], None, None]

SOURCES = {
    "aliyev-empire.zip": "https://atlas.thesentry.org/wp-content/uploads/2024/10/Aliyev-Empire-Data.zip",
    "kiirdom.zip": "https://atlas.thesentry.org/wp-content/uploads/2024/11/Kiirdom-Data.zip",
}
FORMATS = ("%m/%d/%Y",)

EDGES = {
    "SPOUSE": ("Family", "person", "relative"),
    "FATHER": ("Family", "person", "relative"),
    "MOTHER": ("Family", "person", "relative"),
    "SIBLING": ("Family", "person", "relative"),
    "RELATIVE": ("Family", "person", "relative"),
    "ASSOCIATE": ("Associate", "person", "associate"),
    "EMPLOYEE": ("Employment", "employee", "employer"),
    "MANAGER": ("Employment", "employee", "employer"),
    "DIRECTOR": ("Directorship", "director", "organization"),
    "CONTROLS": ("Directorship", "director", "organization"),
    # FIXME: Do we want these schema in active use?
    "SENT_FUNDS": ("Payment", "payer", "beneficiary"),
    "LENT_FUNDS": ("Debt", "creditor", "debtor"),
    "SHAREHOLDER": ("Ownership", "owner", "asset"),
    "OWNER": ("Ownership", "owner", "asset"),
    "MERGED": ("Succession", "predecessor", "successor"),
    "BENEFICIAL_OWNER": ("Ownership", "owner", "asset"),
    "REPRESENTATIVE": ("Representation", "agent", "client"),
    "FOUNDER": ("UnknownLink", "subject", "object"),
    "FACILITATOR": ("UnknownLink", "subject", "object"),
    "SERVICE_PROVIDER": ("Representation", "agent", "client"),
    "CLIENT": ("UnknownLink", "subject", "object"),
}
EDGE_IGNORE = [
    "shareholder",
    "owner",
    "asset",
    "a.name",
    "b.name",
    "associate1",
    "associate2",
    "mother",
    "father",
    "child",
    "employee",
    "employer",
    "sender",
    "reciever",
    "original",
    "merged_company",
    "director",
    "manager",
    "company",
    "representative",
    "represents",
    "facilitator",
    "founder",
    "provider",
    "client",
    "lender",
    "project",
]
NODE_IGNORE = ["project"]


def iter_zf_csv(zf: zipfile.ZipFile, name: str) -> CSVIter:
    with zf.open(name, "r") as fh:
        wrapper = TextIOWrapper(fh)
        wrapper.read(1)
        reader = csv.DictReader(wrapper)
        for row in reader:
            yield row


def crawl_assets(context: Context, rows: CSVIter) -> None:
    for row in rows:
        entity = context.make("Asset")
        entity.id = context.make_slug(row.pop("uuid"))
        entity.add("name", row.pop("name"))
        entity.add("country", row.pop("jurisdiction"))
        entity.add("notes", row.pop("assetType"))
        # entity.add("sourceUrl", row.pop("project"))
        context.emit(entity)
        context.audit_data(row, ignore=NODE_IGNORE)


def crawl_individuals(context: Context, rows: CSVIter) -> None:
    for row in rows:
        entity = context.make("Person")
        entity.id = context.make_slug(row.pop("uuid"))
        entity.add("name", row.pop("name"))
        entity.add("alias", row.pop("alias").split(";"))
        dob = row.pop("date of birth")
        h.apply_date(entity, "birthDate", dob, formats=FORMATS)
        entity.add("nationality", row.pop("nationality").split(";"))
        entity.add("notes", row.pop("info"))
        entity.add("summary", row.pop("citation"))
        # entity.add("sourceUrl", row.pop("project"))
        entity.add("topics", "poi")
        context.emit(entity)
        context.audit_data(row, ignore=NODE_IGNORE)


def crawl_entities(context: Context, rows: CSVIter) -> None:
    for row in rows:
        entity = context.make("Organization")
        entity.id = context.make_slug(row.pop("uuid"))
        entity.add("name", row.pop("name"))
        entity.add("alias", row.pop("alias").split(";"))
        date_of_inc = row.pop("dateOfIncorporation")
        h.apply_date(entity, "incorporationDate", date_of_inc, formats=FORMATS)
        date_of_dis = row.pop("dateOfDissolution")
        h.apply_date(entity, "dissolutionDate", date_of_dis, formats=FORMATS)
        entity.add("jurisdiction", row.pop("jurisdiction").split(";"))
        entity.add("sector", row.pop("industry"))
        entity.add("status", row.pop("status"))
        entity.add("registrationNumber", row.pop("companyNumber"))
        entity.add("notes", row.pop("info"))
        entity.add("summary", row.pop("citation"))
        # entity.add("sourceUrl", row.pop("project"))
        context.emit(entity)
        context.audit_data(row, ignore=NODE_IGNORE)


def crawl_edge(context: Context, edge_type: str, rows: CSVIter) -> None:
    if edge_type == "ADDRESS":
        # FIXME: this will break if an address is linked to an asset
        for row in rows:
            entity = context.make("LegalEntity")
            entity.id = context.make_slug(row.pop("a.uuid"))
            entity.add("address", row.pop("address"))
        return
    if edge_type not in EDGES:
        context.log.warning(f"Unknown edge type: {edge_type}")
        return
    (schema, a_prop, b_prop) = EDGES[edge_type]
    for row in rows:
        # print(row)
        entity = context.make(schema)
        entity.id = context.make_slug(row.pop("r.uuid"))
        entity.add(a_prop, context.make_slug(row.pop("a.uuid")))
        entity.add(b_prop, context.make_slug(row.pop("b.uuid")))
        percentage = row.pop("r.percentage", None)
        if entity.schema.properties.get("percentage") and percentage != "null":
            entity.add("percentage", percentage)
        type_ = row.pop("type(r)").replace("_", " ").capitalize()
        title = row.pop("r.title", None)
        if title is not None and title != "null":
            type_ = title
        if entity.schema.properties.get("role"):
            entity.add("role", type_)
        elif entity.schema.properties.get("relationship"):
            entity.add("relationship", type_)
        h.apply_date(entity, "startDate", row.pop("r.startDate", None), formats=FORMATS)
        h.apply_date(entity, "endDate", row.pop("r.endDate", None), formats=FORMATS)
        h.apply_date(entity, "date", row.pop("r.asOf", None), formats=FORMATS)
        entity.add("summary", row.pop("r.citation", None))
        context.emit(entity)
        context.audit_data(row, ignore=EDGE_IGNORE)


def crawl(context: Context) -> None:
    for file_name, data_url in SOURCES.items():
        path = context.fetch_resource(file_name, data_url)
        context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
        # entity_schema: Dict[str, str] = {}
        context.log.info("Crawling: %r" % path)

        with zipfile.ZipFile(path, "r") as zf:
            for name in zf.namelist():
                if "_NODE_" in name:
                    _, suffix = name.rsplit("_NODE_", 1)
                    node_type = suffix.split(".")[0]
                    rows = iter_zf_csv(zf, name)
                    if node_type == "ASSETS":
                        crawl_assets(context, rows)
                    elif node_type in ("INDIVIDUALS", "PERSON"):
                        crawl_individuals(context, rows)
                    elif node_type in ("ENTITIES", "ENTITY"):
                        crawl_entities(context, rows)
                    else:
                        context.log.warning(f"Unknown node type: {node_type}")

                # for name in zf.namelist():
                if "_EDGE_" in name:
                    rows = iter_zf_csv(zf, name)
                    _, suffix = name.rsplit("_EDGE_", 1)
                    edge_type = suffix.split(".")[0]
                    crawl_edge(context, edge_type, rows)
