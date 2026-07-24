import json
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Static API key hardcoded in the congress.gov.ph frontend bundle (the Axios client
# factory in _next/static/chunks/app/house-members/page-*.js sends it as the
# x-hrep-website-backend header). It ships to every site visitor, so it is not a
# secret. A 401 "Invalid authorization" response means the site rotated the key and
# this constant needs to be updated from the live bundle.
WEBSITE_BACKEND_TOKEN = "cc8bd00d-9b88-4fee-aafe-311c574fcdc1"
TOPICS = ["gov.legislative", "gov.national"]


def district_constituency(memberships: dict[str, Any]) -> str | None:
    """Build a human-readable legislative district from a membership record.

    District seats carry the ordinal district (`dist_desc`, e.g. "6th District"
    or "Lone District") and the province or city it belongs to (`dist_name`),
    combined as e.g. "6th District, City of Manila". Returns None if neither is
    present. The top-level `district` field is only an internal numeric id."""
    parts = [memberships.get("dist_desc"), memberships.get("dist_name")]
    labels = [p for p in parts if p]
    if not labels:
        return None
    return ", ".join(labels)


def fetch_page(context: Context, page: int) -> dict[str, Any]:
    # The "current" endpoint always returns the currently-seated Congress and ignores
    # the `congress` parameter, so we don't pin a term — the dataset follows whichever
    # Congress is in session. crawl() derives and logs which one that is.
    body = {
        "page": page,
        "limit": 100,
        "filter": "",
        "type": "all",
    }
    # No caching: the API replies HTTP 200 with the real status in the body, and the
    # cache key (request_hash) does not include headers — so a cached auth failure
    # could not be invalidated by fixing the token. The roster is only a few pages.
    response = context.fetch_json(
        context.data_url,
        method="POST",
        data=json.dumps(body),
        headers={
            "Content-Type": "application/json",
            "x-hrep-website-backend": WEBSITE_BACKEND_TOKEN,
        },
    )
    status = response.get("status")
    if status == 401:
        raise RuntimeError(
            "API rejected the authorization token (HTTP body status 401: {!r}). The "
            "site likely rotated x-hrep-website-backend; refresh WEBSITE_BACKEND_TOKEN "
            "from the current value in the congress.gov.ph house-members JS bundle.".format(
                response.get("message")
            )
        )
    if status != 200 or not response.get("success") or "data" not in response:
        raise RuntimeError(f"Unexpected API response: {response!r}")
    data: dict[str, Any] = response["data"]
    return data


def crawl_member(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
    congresses: set[str],
) -> None:
    author_id = row.pop("author_id")
    member_type = row.pop("member_type")
    memberships = row.pop("memberships") or {}

    if author_id == "000000":
        # Party-list seat without a named representative yet; nothing to emit until
        # the organisation's nominee is seated.
        context.log.info("Skipping unseated party-list slot", name=row.get("fullname"))
        return

    congress_desc = memberships.get("congress_desc")
    if not congress_desc:
        raise RuntimeError(f"Member {author_id} has no Congress label: {memberships!r}")
    congresses.add(congress_desc)

    person = context.make("Person")
    person.id = context.make_slug(str(row.pop("id")))
    h.apply_name(
        person,
        first_name=row.pop("first_name"),
        middle_name=row.pop("middle_name"),
        last_name=row.pop("last_name"),
        suffix=row.pop("suffix"),
        lang="eng",
    )

    constituency: str | None = None
    if member_type == "Party List Representative":
        # Party-list reps represent a national/sectoral organisation rather than a
        # territorial district; that organisation is their political vehicle.
        person.add("political", memberships.get("party_list_name"), lang="eng")
        person.add("political", memberships.get("party_list_desc"), lang="eng")
    elif member_type == "District Representative":
        # District reps represent a single legislative district, recorded on the
        # occupancy as the constituency (e.g. "6th District, City of Manila").
        constituency = district_constituency(memberships)

    person.add("political", row.pop("party_affilation_desc"), lang="eng")

    # Membership of the House requires natural-born Philippine citizenship for both
    # district and party-list representatives: 1987 Constitution, Article VI, Section 6.
    # https://www.officialgazette.gov.ph/constitutions/1987-constitution/
    person.add("citizenship", "ph")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)
    context.emit(person)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Representatives of the Philippines",
        country="ph",
        topics=TOPICS,
        wikidata_id="Q18002923",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    congresses: set[str] = set()
    page = 0
    while True:
        data = fetch_page(context, page)
        rows = data["rows"]
        if not rows:
            break
        for row in rows:
            crawl_member(context, row, position, categorisation, congresses)
        if page + 1 >= data["pageCount"]:
            break
        page += 1
