import csv
import tarfile
from io import TextIOWrapper
from pathlib import Path
from normality import slugify
from typing import Callable, Optional, Dict

from zavod import Context, Entity
from zavod import helpers as h

Row = Dict[str, str]


def clean(value: Optional[str] = None) -> Optional[str]:
    if value is None:
        return None
    if value.lower().strip() == "null":
        return None
    return value


def make_proxy(context: Context, cw_id: str, row: Row) -> Optional[Entity]:
    """
    The cases detected where we don't find a suitable id are unusual data, so
    it's ok to not return any proxy then.
    """
    proxy_id = context.make_slug(clean(cw_id))
    if proxy_id is None:
        # apparently the row_id matches cw_id in this case
        proxy_id = context.make_slug(clean(row.pop("row_id", None)))

    if proxy_id is not None:
        proxy = context.make("Company")
        proxy.id = proxy_id
        return proxy
    return None


def parse_companies(context: Context, row: Row):
    proxy = make_proxy(context, row.pop("cw_id"), row)
    if proxy is not None:
        proxy.add("name", clean(row.pop("company_name")))
        context.emit(proxy)


def parse_company_info(context: Context, row: Row):
    proxy = make_proxy(context, row.pop("cw_id"), row)
    if proxy is not None:
        proxy.add("name", clean(row.pop("company_name")))
        proxy.add("sector", clean(row.pop("industry_name")))
        proxy.add("sector", clean(row.pop("sector_name")))
        proxy.add("registrationNumber", clean(row.pop("irs_number")))
        context.emit(proxy)


def parse_company_names(context: Context, row: Row):
    proxy = make_proxy(context, row.pop("cw_id"), row)
    if proxy is not None:
        proxy.add("country", clean(row.pop("country_code")))
        name_type = row.pop("source")
        name = clean(row.pop("company_name"))
        if name_type == "cik_former_name":
            proxy.add("previousName", name)
        else:
            proxy.add("name", name)
        if len(proxy.properties):
            context.emit(proxy)


def parse_company_locations(context: Context, row: Row):
    proxy = make_proxy(context, row.pop("cw_id"), row)
    if proxy is not None:
        country_code = clean(row.pop("country_code")) or ""
        proxy.add("country", country_code)
        street = [s for s in (row.pop("street_1"), row.pop("street_2")) if clean(s)]
        street = ", ".join(street)
        address = h.format_address(
            street=street,
            postal_code=clean(row.pop("postal_code")),
            city=clean(row.pop("city")),
            state=clean(row.pop("state")),
            country_code=country_code.lower(),
        )
        # don't add addresses consisting only of placeholder characters:
        if slugify(address) is not None:
            proxy.add("address", address)
        if len(proxy.properties):
            context.emit(proxy)


def parse_company_relations(context: Context, row: Row):
    source = make_proxy(context, row.pop("source_cw_id"), row)
    target = make_proxy(context, row.pop("target_cw_id"), row)
    if source is not None and target is not None:
        target.add("parent", source)
        context.emit(target)
        if len(source.properties):
            context.emit(source)


def parse_relationships(context: Context, row: Row):
    if row.pop("ignore_record") != "0":
        return
    year = clean(row.pop("year"))
    percentage = clean(row.pop("percent"))
    if percentage or year:
        parent = make_proxy(context, row.pop("parent_cw_id"), row)
        child = make_proxy(context, row.pop("cw_id"), row)
        if parent is not None and child is not None:
            child.add("name", clean(row.pop("company_name")))
            rel = context.make("Ownership")
            rel.id = context.make_slug("ownership", parent.id, child.id)
            rel.add("owner", parent)
            rel.add("asset", child)
            rel.add("percentage", percentage)
            rel.add("date", year)
            if len(parent.properties):
                context.emit(parent)
            if len(child.properties):
                context.emit(child)
            context.emit(rel)


def parse_csv(context: Context, data_path: Path, name: str, handler: Callable) -> None:
    context.log.info(f"Parsing `{name}` ...")
    with tarfile.open(data_path, "r:*") as tf:
        member = tf.extractfile(f"corpwatch_api_tables_csv/{name}")
        if member is None:
            raise RuntimeError("Missing file in archive: %s" % name)
        wrapper = TextIOWrapper(member, encoding="utf-8-sig")
        reader = csv.DictReader(wrapper, delimiter="\t")
        idx = 0
        for idx, row in enumerate(reader):
            handler(context, row)
            if idx and idx % 100_000 == 0:
                context.log.info(f"Parse record {idx}...", name=name)
        context.log.info(f"Parsed {idx} rows", name=name)
        member.close()


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.tar.gz", context.data_url)
    parse_csv(context, path, "companies.csv", parse_companies)
    parse_csv(context, path, "company_info.csv", parse_company_info)
    parse_csv(context, path, "company_names.csv", parse_company_names)
    parse_csv(context, path, "company_locations.csv", parse_company_locations)
    parse_csv(context, path, "company_relations.csv", parse_company_relations)
    parse_csv(context, path, "relationships.csv", parse_relationships)
