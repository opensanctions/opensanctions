import re

from zavod import Context, helpers as h

PROGRAM_KEY = "CZ-TERR"
# Announced 17 June 2008, date of effect 17 June 2008
START_DATE = "2008-06-17"

API_BASE_URL = "https://opendata.eselpoint.gov.cz"
JSONLD_HEADERS = {"Accept": "application/ld+json"}


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
    # Fetch the legal act; contains a list of all dated versions of this regulation.
    act_data = context.fetch_json(
        context.data_url, headers=JSONLD_HEADERS, cache_days=1
    )
    # The IRI of the most recent consolidated version of the regulation text.
    latest_version_iri = act_data["má-poslední-znění"]

    # Fetch the version; contains a flat list of all structural fragment IRIs
    # (sections, paragraphs, individual list items, etc.)
    version_data = context.fetch_json(
        f"{API_BASE_URL}/{latest_version_iri}", headers=JSONLD_HEADERS, cache_days=1
    )
    all_fragments = version_data["má-fragment-znění"]

    # Filter for individual list items (bod = "point/item") under the two
    # appendix sections: cast_1 (Part 1) = persons,
    # cast_2 (Part 2) = organizations.
    bod_iris = sorted(f for f in all_fragments if re.search(r"/cast_[12]/bod_\d+$", f))
    # 31 persons (cast_1) + 18 organizations (cast_2)
    assert len(bod_iris) == 49, f"Expected 49 items, got {len(bod_iris)}"

    for bod_iri in bod_iris:
        # Each bod (list item in the version) references a reusable text fragment.
        bod_data = context.fetch_json(
            f"{API_BASE_URL}/{bod_iri}", headers=JSONLD_HEADERS, cache_days=1
        )
        fragment_iri = bod_data["obsahuje-fragment"]

        # The text fragment holds the actual prose content of the list item.
        fragment_data = context.fetch_json(
            f"{API_BASE_URL}/{fragment_iri}", headers=JSONLD_HEADERS, cache_days=1
        )
        raw_text = fragment_data["l-sgov-dat-sbirka-pojem:text-fragmentu"]
        # Strip the <var>N.</var> numbering prefix, leaving the entity description
        details = re.sub(r"<var>.*?</var>", "", raw_text)
        crawl_details(context, details)
