import csv
import shutil
from pathlib import Path
from typing import Any

from rigour.mime.types import CSV

from zavod import Context, helpers as h

LOCAL_PATH = Path(__file__).parent
SPARQL_URL = "https://fedlex.data.admin.ch/sparqlendpoint"
# Every consolidated ordinance whose German title marks it as an asset freeze
# under the Foreign Illicit Assets Act, with its in-force status and the
# applicability date of each consolidated version.
SPARQL_QUERY = """
PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>
SELECT ?abstract ?title ?status ?date WHERE {
  ?expr jolux:title ?title .
  ?abstract jolux:isRealizedBy ?expr .
  ?abstract a jolux:ConsolidationAbstract .
  FILTER(CONTAINS(?title, "Sperrung von Vermögenswerten"))
  OPTIONAL { ?abstract jolux:inForceStatus ?status }
  OPTIONAL {
    ?conso a jolux:Consolidation .
    ?conso jolux:isMemberOf ?abstract .
    ?conso jolux:dateApplicability ?date .
  }
}
"""


def crawl_row(context: Context, row: dict[str, str]) -> None:
    name = row.pop("Name")
    program = row.pop("Program")
    entity = context.make("Person")
    entity.id = context.make_id(program, name)
    entity.add("name", name)
    entity.add("alias", row.pop("Alias").split(";"))
    entity.add("previousName", row.pop("Previous name"))
    h.apply_date(entity, "birthDate", row.pop("Birth date"))
    entity.add("birthPlace", row.pop("Birth place"))
    entity.add("nationality", row.pop("Nationality"))
    entity.add("passportNumber", row.pop("Passport numbers").split(";"))
    entity.add("notes", row.pop("Notes"), lang="deu")
    entity.add("country", row.pop("Country"))

    sanction = h.make_sanction(
        context,
        entity,
        program_key=program,
        start_date=row.pop("Listed"),
        end_date=row.pop("Delisted"),
    )
    sanction.add("sourceUrl", row.pop("Source URL"))
    if h.is_active(sanction):
        entity.add("topics", "sanction")
    context.emit(sanction)
    context.emit(entity)
    context.audit_data(row)


def check_fedlex_versions(context: Context) -> None:
    """Warn when Fedlex shows asset-freeze ordinances or versions we have not reviewed.

    The annexes only change through formal amendment ordinances, each of which
    produces a dated consolidated version on Fedlex. This check compares the
    consolidation dates, the set of freeze ordinances, and their in-force
    status against the reviewed state recorded in the dataset config; any
    difference needs a human to update freezes.csv (see the runbook comment in
    the yml).
    """
    discovery: dict[str, Any] = context.dataset.config.get("discovery", {})
    ordinances: dict[str, Any] = discovery.get("ordinances", {})
    response = context.fetch_json(
        SPARQL_URL,
        params={"query": SPARQL_QUERY},
        headers={"Accept": "application/sparql-results+json"},
        cache_days=2,
    )
    bindings = response["results"]["bindings"]
    if len(bindings) == 0:
        raise ValueError("Fedlex SPARQL query returned no asset-freeze ordinances")

    found: dict[str, dict[str, Any]] = {}
    for binding in bindings:
        eli = binding["abstract"]["value"]
        eli = eli.removeprefix("https://fedlex.data.admin.ch/eli/")
        info = found.setdefault(eli, {"in_force": True, "dates": set()})
        status = binding.get("status", {}).get("value", "")
        # https://fedlex.data.admin.ch/vocabulary/enforcement-status/3 is
        # "No longer in force"; the vocabulary entry is absent while in force.
        if status.endswith("/3"):
            info["in_force"] = False
        if "date" in binding:
            info["dates"].add(binding["date"]["value"])

    for eli, info in found.items():
        config = ordinances.get(eli)
        if config is None:
            context.log.warning(
                "New asset-freeze ordinance on Fedlex not covered by freezes.csv",
                ordinance=f"https://www.fedlex.admin.ch/eli/{eli}/de",
            )
            continue
        if config.get("in_force") != info["in_force"]:
            context.log.warning(
                "Asset-freeze ordinance changed its in-force status",
                ordinance=f"https://www.fedlex.admin.ch/eli/{eli}/de",
                in_force=info["in_force"],
            )
        reviewed = {str(date) for date in config.get("reviewed_versions", [])}
        for date in sorted(info["dates"] - reviewed):
            compact = date.replace("-", "")
            context.log.warning(
                "Unreviewed version of an asset-freeze ordinance",
                date=date,
                version_url=f"https://www.fedlex.admin.ch/eli/{eli}/{compact}/de",
            )

    for eli in ordinances.keys():
        if eli not in found:
            context.log.warning(
                "Known asset-freeze ordinance disappeared from Fedlex",
                ordinance=f"https://www.fedlex.admin.ch/eli/{eli}/de",
            )


def crawl(context: Context) -> None:
    source_file = LOCAL_PATH / "freezes.csv"
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)

    try:
        check_fedlex_versions(context)
    except Exception as exc:
        context.log.warning("Fedlex version check failed", error=str(exc))
