from typing import Any, Dict
import ijson
import zipfile
from urllib.parse import urljoin
from rigour.mime.types import PDF
from rigour.langs import iso_639_alpha3

from zavod import Context
from zavod.archive import dataset_data_path


def get_download_url(context: Context) -> str:
    doc = context.fetch_html(context.data_url)
    for a in doc.findall(".//a"):
        url = urljoin("https://zenodo.org/", a.get("href"))
        if "/records/" in url and "/files/" in url:
            return url
    raise ValueError("No download link found")


def crawl_item(context: Context, item: Dict[str, Any]) -> None:
    ror_uri = item.pop("id")
    entity = context.make("Organization")
    entity.id = context.make_slug(ror_uri.rsplit("/", 1)[-1])
    entity.add("sourceUrl", ror_uri)
    entity.add("name", item.pop("name"))
    entity.add("alias", item.pop("aliases", []))
    for label in item.pop("labels", []):
        lang = iso_639_alpha3(label.get("iso639", ""))
        entity.add("alias", label.get("label"), lang=lang)

    entity.add("weakAlias", item.pop("acronyms"))
    entity.add("sector", item.pop("types"))
    country = item.pop("country", {})
    entity.add("country", country.get("country_code"))
    entity.add("website", item.pop("links", []))
    entity.add("email", item.pop("email_address", []))
    entity.add("status", item.pop("status", None))
    entity.add("incorporationDate", item.pop("established", None))
    for name, values in item.pop("external_ids", {}).items():
        if name == "Wikidata":
            entity.add("wikidataId", values.get("preferred"))
            entity.add("wikidataId", values.get("all"))

    for rel in item.pop("relationships", []):
        rel_type = rel.pop("type")
        rel_other = rel.pop("id")
        other_id = context.make_slug(rel_other.rsplit("/", 1)[-1])
        link = context.lookup("relationship", rel_type)
        if link is None:
            context.log.warn("Unknown relationship type", rel_type=rel_type)
            continue
        rel = context.make(link.schema)
        rel.id = context.make_id(
            max(entity.id, other_id), min(entity.id, other_id), rel_type
        )
        rel.add(link.local, entity.id)
        rel.add(link.remote, other_id)
        rel.add(link.description, rel_type)
        context.emit(rel)

    context.audit_data(item, ignore=["wikipedia_url", "ip_addresses", "addresses"])
    context.emit(entity)


def crawl(context: Context) -> None:
    zip_url = get_download_url(context)
    path = context.fetch_resource("source.zip", zip_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    data_path = dataset_data_path(context.dataset.name)
    with zipfile.ZipFile(path, "r") as zf:
        for name in zf.namelist():
            if name.endswith("-data.json"):
                zf.extract(name, data_path)
                break

    json_path = data_path / name
    if not json_path.exists():
        raise ValueError("No JSON data found in ZIP file")

    with open(json_path, "rb") as fh:
        for item in ijson.items(fh, "item"):
            crawl_item(context, item)
