import io
from pathlib import Path
from typing import Dict, Generator
from csv import DictReader
from zipfile import ZipFile
from normality import stringify
from zavod.logs import get_logger
from followthemoney import model

from zavod import Context, Entity
from zavod import helpers as h

log = get_logger("offshoreleaks")

ENTITIES: Dict[str, Entity] = {}
DATE_FORMATS = [
    "%d-%b-%Y",
    "%b %d, %Y",
    "%Y-%m-%d",
    "%Y",
    "%d/%m/%Y",
    "%d.%m.%Y",
    "%d/%m/%y",
]
NODE_URL = "https://offshoreleaks.icij.org/nodes/%s"


def parse_date(text):
    return h.parse_date(text, DATE_FORMATS)


def parse_countries(text):
    return h.multi_split(text, [";"])


def emit_entity(proxy: Entity):
    assert proxy.id is not None, proxy
    if proxy.id in ENTITIES:
        schemata = [proxy.schema.name, ENTITIES[proxy.id].schema.name]
        if sorted(schemata) == sorted(["Asset", "Organization"]):
            proxy.schema = model.get("Company")
        if sorted(schemata) == sorted(["Asset", "LegalEntity"]):
            proxy.schema = model.get("Company")

        try:
            proxy = ENTITIES[proxy.id].merge(proxy)
        except Exception:
            print(proxy.schema, ENTITIES[proxy.id].schema)
            raise
    ENTITIES[proxy.id] = proxy


def dump_nodes(context: Context):
    context.log.info("Dumping %d nodes to: %s", len(ENTITIES), context.sink)
    for idx, entity in enumerate(ENTITIES.values()):
        assert not entity.schema.abstract, entity
        if entity.schema.name == "Address":
            continue
        context.emit(entity)
        if idx > 0 and idx % 10000 == 0:
            context.log.info("Dumped %d nodes..." % idx)


def read_rows(
    context: Context, zip_path: Path, file_name: str
) -> Generator[Dict[str, str], None, None]:
    with ZipFile(zip_path, "r") as zip:
        with zip.open(file_name) as zfh:
            fh = io.TextIOWrapper(zfh)
            reader = DictReader(fh, delimiter=",", quotechar='"')
            for idx, row in enumerate(reader):
                yield {k: stringify(v) for (k, v) in row.items()}
                if idx > 0 and idx % 10000 == 0:
                    context.log.info("Read %d rows..." % idx, file_name=file_name)


def make_row_entity(context: Context, row: Dict[str, str], schema):
    # node_id = row.pop("id", row.pop("_id", row.pop("node_id", None)))
    node_id = row.pop("node_id", None)
    proxy = context.make(schema)
    proxy.id = context.make_slug(node_id)
    if proxy.id is None:
        context.log.error("No ID: %r", row)
        return
    name = row.pop("name", None)
    proxy.add("name", name)
    former_name = row.pop("former_name", None)
    if name != former_name:
        proxy.add("previousName", former_name)
    original_name = row.pop("original_name", None)
    if original_name != name:
        proxy.add("previousName", original_name)

    proxy.add("icijId", node_id)
    proxy.add("sourceUrl", NODE_URL % node_id)
    proxy.add("legalForm", row.pop("company_type", None))
    proxy.add("legalForm", row.pop("type", None))
    date = parse_date(row.pop("incorporation_date", None))
    proxy.add("incorporationDate", date)
    date = parse_date(row.pop("inactivation_date", None))
    proxy.add("dissolutionDate", date)
    date = parse_date(row.pop("struck_off_date", None))
    proxy.add("dissolutionDate", date)

    if proxy.schema.is_a("Organization"):
        proxy.add("topics", "corp.offshore")

    closed_date = parse_date(row.pop("closed_date", None))
    # if proxy.has("dissolutionDate"):
    #     log.warning("Company has both dissolution date and closed date: %r", proxy)
    proxy.add("dissolutionDate", closed_date)

    dorm_date = parse_date(row.pop("dorm_date", None))
    # if proxy.has("dissolutionDate"):
    #     log.warning("Company has both dissolution date and dorm date: %r", proxy)
    proxy.add("dissolutionDate", dorm_date)

    proxy.add("status", row.pop("status", None))
    proxy.add("publisher", row.pop("sourceID", None))
    proxy.add("notes", row.pop("valid_until", None))
    proxy.add("notes", row.pop("note", None))

    row.pop("jurisdiction", None)
    # countries = parse_countries()
    # proxy.add("jurisdiction", countries)
    countries = parse_countries(row.pop("jurisdiction_description", None))
    proxy.add("jurisdiction", countries)
    proxy.add("address", row.pop("address", None))

    countries = parse_countries(row.pop("country_codes", None))
    proxy.add("country", countries)

    countries = parse_countries(row.pop("countries", None))
    proxy.add("country", countries)
    proxy.add("program", row.pop("service_provider", None))

    proxy.add("registrationNumber", row.pop("ibcRUC", None), quiet=True)

    row.pop("internal_id", None)
    context.audit_data(row)
    emit_entity(proxy)


def make_row_address(context: Context, row: Dict[str, str]):
    node_id = row.pop("node_id", None)
    proxy = context.make("Address")
    proxy.id = context.make_slug(node_id)
    proxy.add("full", row.pop("address", None))

    name = row.pop("name", None)
    proxy.add("full", name)
    # if name is not None:
    #     log.info("Name [%s] => [%s]", proxy.first("full"), name)

    row.pop("country_codes", None)
    countries = parse_countries(row.pop("countries"))
    proxy.add("country", countries)
    proxy.add("summary", row.pop("valid_until", None))
    proxy.add("remarks", row.pop("note", None))
    proxy.add("publisher", row.pop("sourceID", None))

    context.audit_data(row)
    emit_entity(proxy)


LINK_SEEN = set()


def make_row_relationship(context: Context, row: Dict[str, str]):
    # print(row)
    # return
    _type = row.pop("rel_type")
    _start = row.pop("node_id_start")
    _end = row.pop("node_id_end")
    start = context.make_slug(_start)
    assert start is not None, _start
    start_ent = ENTITIES.get(start)
    end = context.make_slug(_end)
    assert end is not None, _end
    end_ent = ENTITIES.get(end)
    link = row.pop("link", None)
    source_id = row.pop("sourceID", None)
    start_date = parse_date(row.pop("start_date"))
    end_date = parse_date(row.pop("end_date"))

    try:
        res = context.lookup("relationships", link)
    except Exception:
        context.log.exception("Unknown link: %s" % link)
        return

    if start_ent is None or end_ent is None:
        return

    if res is None:
        if link not in LINK_SEEN:
            # log.warning("Unknown link type: %s (%s, %s)", link, _type, row)
            LINK_SEEN.add(link)
        return

    if start_ent.schema.name == "Address":
        return

    if end_ent.schema.name == "Address" and start_ent.schema.is_a("Thing"):
        start_ent.add("address", end_ent.get("full"))
        start_ent.add("country", end_ent.get("country"))
        return

    if res.address:
        context.log.warn(
            "Address is not an address",
            start=start_ent,
            end=end_ent,
            link=link,
            type=_type,
        )
        return

    if end_ent is not None and end_ent.schema.name == "Address":
        context.log.warn("End is addr", link=link, end=end_ent)

    if res.schema is not None:
        rel = context.make(res.schema)
        rel.id = context.make_slug(_start, _end, link)
        rel.add("startDate", start_date)
        rel.add("endDate", end_date)
        rel.add(res.status, row.pop("status"))
        rel.add(res.link, link)
        rel.add("publisher", source_id)
        rel.add(res.start, start)
        rel.add(res.end, end)
        # emit_entity(rel)
        context.emit(rel)

        # this turns legalentity into organization in some cases
        start_ent = context.make(rel.schema.get(res.start).range)
        start_ent.id = start
        emit_entity(start_ent)

        end_ent = context.make(rel.schema.get(res.end).range)
        end_ent.id = end
        emit_entity(end_ent)

    context.audit_data(row)


def crawl(context: Context):
    zip_path = context.fetch_resource("oldb.zip", context.data_url)
    context.log.info("Loading: nodes-entities.csv...")
    for row in read_rows(context, zip_path, "nodes-entities.csv"):
        make_row_entity(context, row, "Company")

    context.log.info("Loading: nodes-officers.csv...")
    for row in read_rows(context, zip_path, "nodes-officers.csv"):
        make_row_entity(context, row, "LegalEntity")

    context.log.info("Loading: nodes-intermediaries.csv...")
    for row in read_rows(context, zip_path, "nodes-intermediaries.csv"):
        make_row_entity(context, row, "LegalEntity")

    context.log.info("Loading: nodes-others.csv...")
    for row in read_rows(context, zip_path, "nodes-others.csv"):
        make_row_entity(context, row, "LegalEntity")

    context.log.info("Loading: nodes-addresses.csv...")
    for row in read_rows(context, zip_path, "nodes-addresses.csv"):
        make_row_address(context, row)

    context.log.info("Loading: relationships.csv...")
    for row in read_rows(context, zip_path, "relationships.csv"):
        make_row_relationship(context, row)

    dump_nodes(context)
