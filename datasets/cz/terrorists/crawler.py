import re
from urllib.parse import quote

from zavod import Context, helpers as h

PROGRAM_KEY = "CZ-TERR"
# Announced 17 June 2008, date of effect 17 June 2008
START_DATE = "2008-06-17"

OPENDATA_BASE = "https://opendata.eselpoint.gov.cz"
JSONLD_HEADERS = {"Accept": "application/ld+json"}

E_SBIRKA_BASE = "https://e-sbirka.gov.cz"


def crawl_details(context: Context, details: str) -> None:
    result = context.lookup("details", details)
    if not result or not result.details:
        context.log.warning("Details are not parsed", details=details)
        return

    override = result.details[0]

    entity = context.make(override.get("schema"))
    if entity.schema.is_a("Organization"):
        name_org = override.get("name")
        entity.id = context.make_id(name_org)
        entity.add("name", name_org)
        entity.add("alias", override.get("alias"))
        entity.add("notes", override.get("notes"))
    else:
        first_name = override.get("first_name")
        last_name = override.get("last_name")
        entity.id = context.make_id(first_name, last_name)
        h.apply_name(entity, first_name=first_name, last_name=last_name)
        h.apply_date(entity, "birthDate", override.get("dob"))
        entity.add("birthPlace", override.get("pob"))
        entity.add("idNumber", override.get("id_num"))
        entity.add("position", override.get("position"))
    # Reflects both a sanctions list and terrorist designations
    entity.add("topics", ["sanction", "crime.terror"])

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
    h.apply_date(sanction, "startDate", START_DATE)

    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    # Discover the latest consolidated version via the opendata JSON-LD API.
    # `má-poslední-znění` is an IRI like `esel-esb/eli/cz/sb/2008/210/2009-04-01`.
    act_data = context.fetch_json(
        context.data_url, headers=JSONLD_HEADERS, cache_days=1
    )
    latest_iri = act_data["má-poslední-znění"]
    # Drop the `esel-esb/eli/cz` prefix to get the ELI path documented for
    # e-Sbírka's stálé URL: `/sb/{rok}/{cislo}/{rrrr-mm-dd}`.
    prefix = "esel-esb/eli/cz"
    if not latest_iri.startswith(prefix):
        raise RuntimeError(f"Unexpected latest-version IRI: {latest_iri}")
    eli_path = latest_iri[len(prefix) :]

    # Resolve the ELI path to the numeric document ID.
    doc_id = context.fetch_json(
        f"{E_SBIRKA_BASE}/sbr-cache/dokumenty-sbirky/{quote(eli_path, safe='')}/id",
        cache_days=1,
    )

    # Request generation of the informative-version JSON, which returns the
    # UUID of the generated file. For a static regulation like this the file
    # is served from cache and comes back immediately with stavPozadavku=OK.
    request = context.fetch_json(
        f"{E_SBIRKA_BASE}/sbr-cache/stahni/informativni-zneni/{doc_id}/JSON",
        cache_days=1,
    )
    if request.get("stavPozadavku") != "OK":
        raise RuntimeError(f"Unexpected download request state: {request}")
    file_id = request["id"]

    document = context.fetch_json(
        f"{E_SBIRKA_BASE}/souborove-sluzby/soubory/{file_id}", cache_days=1
    )
    # `fragmenty` is a flat list of structural fragments (sections, headings,
    # list items, ...). Each individual list item under the appendix is typed
    # `Bod_Dd` and holds the raw entity description in its `xhtml` field.
    bods = [f for f in document["fragmenty"] if f.get("typ") == "Bod_Dd"]
    # 31 persons (Part 1) + 18 organizations (Part 2)
    assert len(bods) == 49, f"Expected 49 items, got {len(bods)}"

    for bod in bods:
        # Strip the <var>N.</var> numbering prefix, leaving the entity description.
        details = re.sub(r"<var>.*?</var>", "", bod["xhtml"])
        crawl_details(context, details)
