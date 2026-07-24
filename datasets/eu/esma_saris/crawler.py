import csv
import time
from pathlib import Path

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "EU-ESMA"

SEARCH_URL = "https://registers.esma.europa.eu/publication/searchRegister/doMainSearch"
# The export endpoint requires a server-side session that is established by the
# search POST below. Without it, the endpoint returns an HTML page with a 200
# status, which would otherwise be silently parsed as CSV.
FETCH_ATTEMPTS = 3
FETCH_SLEEP = 10


def fetch_source(context: Context) -> Path:
    """Establish the ESMA search session and download the export CSV.

    The export only returns CSV once the ``doMainSearch`` POST has set a session
    cookie; otherwise it serves an HTML landing page with a 200 status. ESMA
    occasionally drops the handshake on a given run, so retry a few times and
    validate that what we fetched is actually the CSV before handing it on.
    """
    data = {
        "core": "esma_registers_saris_new",
        "pagingSize": "10",
        "start": 0,
        "keyword": "",
        "sortField": "effectiveFrom asc",
        "criteria": [],
        "wt": "json",
    }
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        # Set the session cookie used by the export endpoint.
        context.http.post(SEARCH_URL, json=data)
        source_file = context.fetch_resource("source.csv", context.data_url)
        with open(source_file) as f:
            header = f.readline()
        if "instrumentIdentifier" in header:
            return source_file
        # We got the HTML landing page (or some other non-CSV response).
        # Drop the cached file so the next attempt re-fetches.
        context.log.warn(
            "ESMA export did not return CSV, session handshake likely failed",
            attempt=attempt,
            header=header[:200],
        )
        source_file.unlink(missing_ok=True)
        if attempt < FETCH_ATTEMPTS:
            time.sleep(FETCH_SLEEP)
    raise RuntimeError(
        "ESMA export returned HTML, not CSV, after "
        f"{FETCH_ATTEMPTS} attempts (session handshake failed)"
    )


def crawl(context: Context) -> None:
    source_file = fetch_source(context)
    with open(source_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            isin = row.pop("instrumentIdentifier")
            if isin is None:
                context.log.warn("No ISIN", row=row)
                return
            entity = h.make_security(context, isin)
            entity.add("name", row.pop("instrumentFullName", isin))

            sanction = h.make_sanction(
                context, entity, key=row.pop("id"), program_key=PROGRAM_KEY
            )
            sanction.add("provisions", row.pop("actionType"))
            reason = row.pop("reasonsForTheAction")
            sanction.add("reason", reason)
            sanction.add("description", row.pop("comments"))
            sanction.add("startDate", row.pop("effectiveFrom"))
            sanction.add("country", row.pop("memberStateOfNotifyingCA"))
            sanction.set("authority", row.pop("notifyingCA"))
            end_date = row.pop("effectiveTo")
            sanction.add("endDate", end_date)
            if not end_date:
                topic = context.lookup_value("reason_topic", reason)
                if topic is None:
                    context.log.warn("No topic defined for reason", reason=reason)
                entity.add("topics", topic)
            context.emit(sanction)
            issuer_id = row.pop("issuer", "").strip()
            if issuer_id != "":
                issuer = context.make("Organization")
                if len(issuer_id) == 20:
                    issuer.id = f"lei-{issuer_id}"
                    issuer.add("leiCode", issuer_id)
                else:
                    issuer.id = context.make_id(issuer_id)
                issuer.add("name", row.pop("issuerName"))
                context.emit(issuer)
                entity.add("issuer", issuer)

            context.emit(entity)
            context.audit_data(
                row,
                ignore=[
                    "sufficientlyRelatedInstrument",
                    "otherRelatedInstrument",
                    "historicalStatus",
                    "markets",
                    "timestamp",
                    "onGoing",
                ],
            )
