import hashlib
import hmac
import os
import re
import uuid
from base64 import b64encode
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import orjson

from zavod import Context, settings
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.extract.zyte_api import ZyteAPIRequest
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)
from zavod.stateful.review import assert_all_accepted

# Above the 580 seats plus mid-term replacements: one page per legislature.
PAGE_SIZE = 1000

# Taken from the site's axios interceptor. On 401/403, re-extract them by grepping
# /_next/static/chunks/*.js for "x-api-signature".
API_TOKEN = os.environ.get("OPENSANCTIONS_ID_DPR_API_TOKEN")
SIGNING_SECRET = os.environ.get("OPENSANCTIONS_ID_DPR_SIGNING_SECRET")

# Legislature labels, e.g. "Periode 2024 - 2029".
PERIODE_RE = re.compile(r"(?:Periode\s+)?(?P<start>\d{4})\s*-\s*(?P<end>\d{4})")

# Trimmed from the site's own query; formatted with page size and legislature ID.
ROSTER_QUERY = """
{
  getDaftarRiwayatAnggota(
    first: %d
    wherePeriode: { column: ID, operator: EQ, value: %d }
  ) {
    data {
      idAnggota
      statusOff
      dapil { dapil }
      anggota { id nama tempatLahir tanggalLahir }
      riwayatFraksi { fraksi { fraksi } }
    }
  }
}
"""

MEMBER_URL = (
    "https://www.dpr.go.id/en/tentang-dpr/informasi-anggota-dewan/detail-anggota/"
)


@dataclass
class Legislature:
    id: int
    start: str
    end: str


def sign_request(body: bytes) -> dict[str, str]:
    """Build the signing headers required by the /gql endpoint."""
    assert API_TOKEN is not None, "OPENSANCTIONS_ID_DPR_API_TOKEN is not set"
    assert SIGNING_SECRET is not None, "OPENSANCTIONS_ID_DPR_SIGNING_SECRET is not set"
    request_at = datetime.now(UTC).isoformat(timespec="milliseconds")[:-6] + "Z"
    request_id = str(uuid.uuid4())
    request_body = hashlib.sha256(body).hexdigest()
    message = f"POST:{request_body}:{request_at}:{request_id}"
    digest = hmac.new(SIGNING_SECRET.encode(), message.encode(), "sha256").hexdigest()
    signature = b64encode(digest.encode()).decode()
    return {
        "x-api-token": API_TOKEN,
        "x-request-at": request_at,
        "x-request-id": request_id,
        "x-request-body": request_body,
        "x-api-signature": signature,
    }


def query_gql(context: Context, query: str) -> Any:
    """Send a signed GraphQL request and return its data."""
    body = orjson.dumps({"query": query})
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        **sign_request(body),
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
        # Signatures are single-use, so never replay from cache.
        cache_days=None,
    )
    data = orjson.loads(result.response_text)
    if "errors" in data:
        raise RuntimeError(f"GraphQL errors for {query!r}: {data['errors']}")
    return data["data"]


def fetch_legislatures(context: Context) -> list[Legislature]:
    """Fetch the legislative periods and parse their term years."""
    legislatures: list[Legislature] = []
    for periode in query_gql(context, "{ getAllPeriode { id data } }")["getAllPeriode"]:
        match = PERIODE_RE.fullmatch(periode["data"].strip())
        if match is None:
            raise ValueError(f"Cannot parse legislature period: {periode!r}")
        legislatures.append(
            Legislature(
                id=int(periode["id"]),
                start=match.group("start"),
                end=match.group("end"),
            )
        )
    return legislatures


def fetch_roster(context: Context, legislature: Legislature) -> list[dict[str, Any]]:
    """Fetch the full roster of one legislature."""
    query = ROSTER_QUERY % (PAGE_SIZE, legislature.id)
    roster = query_gql(context, query)["getDaftarRiwayatAnggota"]
    members: list[dict[str, Any]] = roster["data"]
    return members


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    legislature: Legislature,
    member: dict[str, Any],
) -> None:
    member_data = member["anggota"]
    if member_data is None:
        # A few historical rows point to member records removed from the site.
        context.log.warning(
            "Skipping roster record with missing member details",
            period_id=legislature.id,
            id_anggota=member["idAnggota"],
        )
        return
    member_id = str(member_data["id"])

    person = context.make("Person")
    person.id = context.make_slug(member_id)

    raw_name = member_data["nama"]
    h.apply_reviewed_name_string(
        context,
        person,
        string=h.strip_name_titles(context, raw_name),
        lang="ind",
        llm_cleaning=True,
    )
    h.apply_date(person, "birthDate", member_data["tanggalLahir"])
    person.add("birthPlace", member_data["tempatLahir"], lang="ind")
    # DPR members must be Indonesian citizens (Law No. 7 of 2017 on General
    # Elections, Article 240 paragraph (1)). https://peraturan.bpk.go.id/Details/37644
    person.add("citizenship", "id")
    faction = member["riwayatFraksi"]
    if faction is not None and faction["fraksi"] is not None:
        person.add("political", faction["fraksi"]["fraksi"], lang="ind")
    # e.g. "Dr-H-C-PUAN-MAHARANI-287"
    slug = re.sub(r"[^A-Za-z0-9]+", "-", raw_name)
    person.add("sourceUrl", f"{MEMBER_URL}{slug}-{member_id}")

    status = None
    # Ended terms are left to the period end; their statusOff is often blank.
    if legislature.end >= str(settings.RUN_TIME.year):
        value = context.lookup_value(
            "occupancy_status", member["statusOff"], "unknown", warn_unmatched=True
        )
        status = OccupancyStatus(value)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=legislature.start,
        period_end=legislature.end,
        status=status,
    )
    if occupancy is None:
        return
    constituency = member["dapil"]
    if constituency is not None:
        occupancy.add("constituency", constituency["dapil"], lang="ind")
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

    for legislature in fetch_legislatures(context):
        if legislature.end < h.earliest_term_start(categorisation.topics):
            continue
        for member in fetch_roster(context, legislature):
            crawl_member(context, position, categorisation, legislature, member)

    assert_all_accepted(context, raise_on_unaccepted=False)
