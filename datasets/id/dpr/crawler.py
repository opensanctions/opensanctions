import hashlib
import hmac
import uuid
from base64 import b64encode
from datetime import UTC, datetime
from itertools import count
from typing import Any

import orjson

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.extract.zyte_api import ZyteAPIRequest
from zavod.stateful.positions import PositionCategorisation, categorise


# Period 6 == the current (2024-2029) term. Bump for historical terms.
PERIODE_ID = 6
# UI paginates at 12; a larger page size cuts the number of signed requests.
PAGE_SIZE = 100

# The frontend signs every GraphQL request with a static token and a hardcoded HMAC
# secret, both read from the site's axios interceptor. If the endpoint starts
# returning 401/403, they have likely rotated: re-extract by grepping the site's JS
# bundles (/_next/static/chunks/*.js) for "x-api-signature".
API_TOKEN = "48a07687-2a14-4647-9d42-23d7f8ebfa45"
SIGNING_SECRET = "LfmqpWYMaEuQA42LcDvmgbBgG4NDmZp73yr8G8pZ"

# The roster query, a trimmed subset of the site's own getDaftarRiwayatAnggota
# operation (root field, variable input types and enum columns taken verbatim from
# the captured request; committee/photo fields we don't map are omitted).
ROSTER_QUERY = """
query getDaftarRiwayatAnggota(
  $page: Int
  $first: Int!
  $where: QueryGetDaftarRiwayatAnggotaWhereWhereConditions
  $wherePeriode: QueryGetDaftarRiwayatAnggotaWherePeriodeWhereHasConditions
  $orderBy: [QueryGetDaftarRiwayatAnggotaOrderByRelationOrderByClause!]
) {
  getDaftarRiwayatAnggota(
    page: $page
    first: $first
    where: $where
    wherePeriode: $wherePeriode
    orderBy: $orderBy
  ) {
    paginatorInfo { total currentPage lastPage hasMorePages perPage }
    data {
      idPeriode
      noAnggota
      idAnggota
      dapil { id dapil }
      anggota { nama id photofileLink }
      riwayatFraksi { idFraksi fraksi { id fraksi singkatan } }
    }
  }
}
""".strip()


def sign_request(method: str, body: bytes) -> dict[str, str]:
    """Build the five signing headers the /gql endpoint requires.

    Reproduces the site's axios request interceptor exactly:

      x-request-body  = sha256(body).hexdigest()
      message         = "METHOD:x-request-body:x-request-at:x-request-id"
      x-api-signature = base64(hmac_sha256(message, SIGNING_SECRET).hexdigest())

    The body digest is bound into the signature, so ``body`` must be the exact bytes
    sent. The timestamp mirrors JavaScript's ``Date.toISOString()`` (millisecond
    precision, UTC "Z"); the server rejects stale timestamps, so signatures cannot be
    cached or replayed.
    """
    now = datetime.now(UTC)
    request_at = f"{now:%Y-%m-%dT%H:%M:%S}.{now.microsecond // 1000:03d}Z"
    request_id = str(uuid.uuid4())
    request_body = hashlib.sha256(body).hexdigest()
    message = f"{method.upper()}:{request_body}:{request_at}:{request_id}"
    digest = hmac.new(
        SIGNING_SECRET.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    signature = b64encode(digest.encode("utf-8")).decode("utf-8")
    return {
        "x-api-token": API_TOKEN,
        "x-request-at": request_at,
        "x-request-id": request_id,
        "x-request-body": request_body,
        "x-api-signature": signature,
    }


def fetch_page(context: Context, page: int) -> dict[str, Any]:
    """Fetch one page of the roster from the GraphQL endpoint."""
    payload = {
        "query": ROSTER_QUERY,
        "variables": {
            "page": page,
            "first": PAGE_SIZE,
            # Only currently serving members; excludes those whose term ended mid-period
            # (e.g. replaced via Pergantian Antar Waktu), who are not current MPs.
            "where": {
                "column": "STATUS_OFF",
                "operator": "EQ",
                "value": "Dalam masa jabatan",
            },
            "wherePeriode": {"column": "ID", "operator": "EQ", "value": PERIODE_ID},
            "orderBy": [{"column": "NO_ANGGOTA", "order": "ASC"}],
        },
    }
    body = orjson.dumps(payload)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        **sign_request("POST", body),
    }
    result = zyte_api.fetch(
        context,
        ZyteAPIRequest(
            url=context.data_url,
            method="POST",
            body=body,
            headers=headers,
            geolocation="id",
        ),
        # Signatures are timestamped and single-use, so a signed request must not be
        # replayed from cache.
        cache_days=None,
    )
    data = orjson.loads(result.response_text)
    if "errors" in data:
        raise RuntimeError(f"GraphQL errors on page {page}: {data['errors']}")
    roster = data["data"]["getDaftarRiwayatAnggota"]
    assert isinstance(roster, dict), roster
    return roster


def crawl_member(
    context: Context,
    member: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    member_data = member.get("anggota")
    assert member_data

    person = context.make("Person")
    person.id = context.make_slug(str(member_data.get("id")))

    raw_name = member_data.get("nama")
    clean_name = h.strip_name_titles(context, raw_name)
    person.add(
        "name",
        clean_name,
        lang="ind",
        original_value=raw_name if clean_name != raw_name else None,
    )
    # DPR members must be Indonesian citizens (Law No. 7 of 2017 on General
    # Elections, Article 240 paragraph (1)). https://peraturan.bpk.go.id/Details/37644
    person.add("citizenship", "id")
    person.add("political", member["riwayatFraksi"]["fraksi"]["fraksi"])

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", member["dapil"]["dapil"])
    context.emit(person)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the People's Representative Council of Indonesia",
        country="id",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328632",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    for page in count(start=1):
        roster = fetch_page(context, page)
        for member in roster["data"]:
            crawl_member(context, member, position, categorisation)
        if not roster["paginatorInfo"]["hasMorePages"]:
            break
