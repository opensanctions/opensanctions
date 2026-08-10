import csv
from pathlib import Path

from zavod import Context, helpers as h
from zavod.extract import zyte_api

RESOLUTIONS_CSV = Path(__file__).parent / "resolutions.csv"
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


def crawl_resolutions(context: Context) -> None:
    # The spreadsheet at data.url is a hand-maintained extraction of the designations
    # in the AMLC's terrorism financing resolutions, so a newly published resolution
    # has to reach a human rather than silently going missing from the dataset.
    # Rewriting resolutions.csv on every run turns one into a reviewable diff: check
    # the new resolution, update the spreadsheet, then commit the updated CSV.
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
    published: dict[str, str] = {}
    for element in h.xpath_elements(doc, RESOLUTION_XPATH):
        title = h.element_text(element)
        # The XPath guarantees each heading opens with its resolution id.
        published[title.split(" ", 1)[0]] = title

    with open(RESOLUTIONS_CSV) as fh:
        known = {row["resolution_id"]: row["title"] for row in csv.DictReader(fh)}
    if published != known:
        context.log.warning(
            "Published AMLC resolutions changed. Reconcile the source spreadsheet "
            "with the resolutions, then commit the updated resolutions.csv.",
            added=sorted(published.keys() - known.keys()),
            removed=sorted(known.keys() - published.keys()),
        )

    with open(RESOLUTIONS_CSV, "w") as fh:
        writer = csv.writer(fh)
        writer.writerow(["resolution_id", "title"])
        # The ids are zero-padded, so this is the newest-first order of the source.
        writer.writerows(sorted(published.items(), reverse=True))


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
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
    crawl_resolutions(context)
