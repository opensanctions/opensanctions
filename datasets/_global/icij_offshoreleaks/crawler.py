import io
from pathlib import Path
from typing import Dict, Generator, List
from csv import DictReader
from zipfile import ZipFile
from normality import stringify
from followthemoney import model

from zavod import Context, Entity
from zavod import helpers as h

SCHEMATA: Dict[str, str] = {}
ADDRESSES_FULL: Dict[str, List[str]] = {}
ADDRESSES_COUNTRIES: Dict[str, List[str]] = {}


def parse_countries(text):
    return h.multi_split(text, [";"])


def emit_entity(context: Context, proxy: Entity):
    assert proxy.id is not None, proxy
    if proxy.id in SCHEMATA:
        schemata = [proxy.schema.name, SCHEMATA[proxy.id]]
        if sorted(schemata) == sorted(["Asset", "Organization"]):
            proxy.schema = model.get("Company")
        if sorted(schemata) == sorted(["Asset", "LegalEntity"]):
            proxy.schema = model.get("Company")

        try:
            proxy.schema = model.common_schema(proxy.schema, SCHEMATA[proxy.id])
        except Exception:
            print(proxy.schema, SCHEMATA[proxy.id])
            raise
    SCHEMATA[proxy.id] = proxy.schema.name
    context.emit(proxy)


def read_rows(
    context: Context, zip_path: Path, file_name: str
) -> Generator[Dict[str, str], None, None]:
    with ZipFile(zip_path, "r") as zip:
        with zip.open(file_name) as zfh:
            fh = io.TextIOWrapper(zfh)
            reader = DictReader(fh, delimiter=",", quotechar='"')
            for idx, row in enumerate(reader):
                yield {k: stringify(v) for (k, v) in row.items()}
                # if idx > 0 and idx % 10000 == 0:
                #     context.log.info("Read %d rows..." % idx, file_name=file_name)


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
    proxy.add("legalForm", row.pop("company_type", None))
    proxy.add("legalForm", row.pop("type", None))
    h.apply_date(proxy, "incorporationDate", row.pop("incorporation_date", None))
    h.apply_date(proxy, "dissolutionDate", row.pop("inactivation_date", None))
    h.apply_date(proxy, "dissolutionDate", row.pop("struck_off_date", None))

    if proxy.schema.is_a("Organization"):
        proxy.add("topics", "corp.offshore")

    h.apply_date(proxy, "dissolutionDate", row.pop("closed_date", None))
    h.apply_date(proxy, "dissolutionDate", row.pop("dorm_date", None))

    proxy.add("status", row.pop("status", None))
    proxy.add("publisher", row.pop("sourceID", None))
    proxy.add("notes", row.pop("valid_until", None))
    proxy.add("notes", row.pop("note", None))
    proxy.add("sourceUrl", f"https://offshoreleaks.icij.org/nodes/{node_id}")

    row.pop("jurisdiction", None)
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
    emit_entity(context, proxy)


def make_row_address(context: Context, row: Dict[str, str]):
    node_id = row.pop("node_id", None)
    ADDRESSES_COUNTRIES[node_id] = parse_countries(row.pop("countries"))
    ADDRESSES_FULL[node_id] = [row.pop("address", None), row.pop("name", None)]
    # proxy = context.make("Address")
    # proxy.id = context.make_slug(node_id)
    # proxy.add(
    #     "full",
    # )

    # name = row.pop("name", None)
    # proxy.add("full", name)
    # # if name is not None:
    # #     log.info("Name [%s] => [%s]", proxy.first("full"), name)

    # row.pop("country_codes", None)
    # countries = parse_countries(row.pop("countries"))
    # proxy.add("country", countries)
    # proxy.add("summary", row.pop("valid_until", None))
    # proxy.add("remarks", row.pop("note", None))
    # proxy.add("publisher", row.pop("sourceID", None))

    context.audit_data(row, ignore=["sourceID", "note", "valid_until", "country_codes"])
    # emit_entity(context, proxy)


LINK_SEEN = set()


def make_row_relationship(context: Context, row: Dict[str, str]):
    _type = row.pop("rel_type")
    _start = row.pop("node_id_start")
    _end = row.pop("node_id_end")
    if _start in ADDRESSES_FULL:
        if _type not in ("same_as", "same_address_as"):
            context.log.warn(
                "Start is addr",
                type=_type,
                start=_start,
                end=_end,
                is_end_addr=_end in ADDRESSES_FULL,
            )
        return

    start = context.make_slug(_start)
    assert start is not None, _start
    start_schema = SCHEMATA.get(start)
    start_ent = context.make(start_schema)
    start_ent.id = start

    if _end in ADDRESSES_FULL:
        start_ent.add("address", ADDRESSES_FULL[_end])
        start_ent.add("country", ADDRESSES_COUNTRIES[_end])
        return

    end = context.make_slug(_end)
    assert end is not None, _end
    end_schema = SCHEMATA.get(end)
    end_ent = context.make(end_schema)
    end_ent.id = end
    link = row.pop("link", None)
    source_id = row.pop("sourceID", None)

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

    if res.skip is True:
        return

    # if start_ent.schema.name == "Address":
    #     context.log.info("Start is addr", link=link, end=end_ent)
    #     return

    # if end_ent.schema.name == "Address" and start_ent.schema.is_a("Thing"):
    #     start_ent.add("address", end_ent.get("full"))
    #     start_ent.add("country", end_ent.get("country"))
    #     return

    # if res.address:
    #     context.log.warn(
    #         "Address is not an address",
    #         start=start_ent,
    #         end=end_ent,
    #         link=link,
    #         type=_type,
    #     )
    #     return

    if end_ent is not None and end_ent.schema.name == "Address":
        context.log.warn("End is addr", link=link, end=end_ent)

    if res.schema is not None:
        rel = context.make(res.schema)
        rel.id = context.make_slug(_start, _end, link)
        h.apply_date(rel, "startDate", row.pop("start_date"))
        h.apply_date(rel, "endDate", row.pop("end_date"))
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
        start_ent.add("sourceUrl", f"https://offshoreleaks.icij.org/nodes/{_start}")
        emit_entity(context, start_ent)

        end_ent = context.make(rel.schema.get(res.end).range)
        end_ent.id = end
        end_ent.add("sourceUrl", f"https://offshoreleaks.icij.org/nodes/{_end}")
        emit_entity(context, end_ent)

    # row = {k: v for k, v in row.items() if v is not None}
    # if len(row):
    #     context.log.warning(
    #         "Unused data: %r" % row,
    #         link=link,
    #         type=_type,
    #         start=_start,
    #         end=_end,
    #         res=res.schema,
    #     )
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
