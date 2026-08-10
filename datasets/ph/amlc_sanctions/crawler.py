import csv
import re

from zavod import Context, helpers as h
from zavod.extract import zyte_api

# The spreadsheet at data.url is a hand-maintained extraction of the designations in
# the AMLC's terrorism financing resolutions. Watch the published list of resolutions
# so that a new one forces a review instead of silently going missing here.
RESOLUTION_ID = re.compile(r"^TF-\d+")
# The cards are rendered client-side, well after the document has loaded, so the
# browser has to be told to wait for them rather than snapshotting the empty shell.
RESOLUTION_XPATH = ".//h3[starts-with(normalize-space(text()), 'TF-')]"
ACTIONS = [
    {
        "action": "waitForSelector",
        "selector": {"type": "xpath", "value": RESOLUTION_XPATH},
        "timeout": 15,
    },
]
ACKNOWLEDGED_RESOLUTIONS = {
    "TF-033",
    "TF-034",
    "TF-035",
    "TF-039",
    "TF-040",
    "TF-041",
    "TF-042",
    "TF-050",
    "TF-055",
    "TF-056",
    "TF-063",
    "TF-064",
    "TF-067",
    "TF-068",
    "TF-069",
    "TF-076",
    "TF-086",
    "TF-087",
    "TF-088",
    "TF-090",
    "TF-102",
    "TF-104",
    "TF-108",
    "TF-112",
    "TF-113",
    "TF-114",
}


def check_resolutions(context: Context) -> None:
    """Warn when the published resolutions differ from the reviewed spreadsheet."""
    assert context.dataset.url is not None
    # The validator doubles as the guarantee that the list actually rendered.
    doc = zyte_api.fetch_html(
        context,
        context.dataset.url,
        RESOLUTION_XPATH,
        actions=ACTIONS,
        cache_days=1,
        javascript=True,
        geolocation="PH",
    )
    published = set()
    for heading in h.xpath_elements(doc, ".//h3"):
        match = RESOLUTION_ID.match(h.element_text(heading))
        if match is not None:
            published.add(match.group(0))

    if published != ACKNOWLEDGED_RESOLUTIONS:
        context.log.warning(
            "Published AMLC resolutions changed. Reconcile the source spreadsheet "
            "with the resolutions, then acknowledge them in the crawler.",
            added=sorted(published - ACKNOWLEDGED_RESOLUTIONS),
            removed=sorted(ACKNOWLEDGED_RESOLUTIONS - published),
        )


def crawl_row(context: Context, row: dict[str, str]) -> None:
    name = row.pop("name")
    name_raw = row.pop("original_string")
    alias = row.pop("alias")
    resolution_no = row.pop("resolution_no")
    program = row.pop("program")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, resolution_no)
    entity.add("name", name.split(";"), original_value=name_raw)
    entity.add("alias", alias.split(";") if alias else None, original_value=name_raw)
    entity.add("topics", "sanction")
    entity.add("country", "ph")
    entity.add("sourceUrl", row.pop("source_url"))
    entity.add("sourceUrl", row.pop("main_source_url"))
    context.emit(entity)

    sanction = h.make_sanction(
        context,
        entity,
        program_name=program,
        program_key=h.lookup_sanction_program_key(context, program),
    )
    sanction.add("program", resolution_no)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    check_resolutions(context)

    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
