import hashlib
import hmac
import re
import uuid
from base64 import b64encode
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


def sign_request(body: bytes) -> dict[str, str]:
    """Build the signing headers required by the /gql endpoint."""
    request_at = datetime.now(UTC).isoformat(timespec="milliseconds")[:-6] + "Z"
    request_id = str(uuid.uuid4())
    request_body = hashlib.sha256(body).hexdigest()
    message = f"POST:{request_body}:{request_at}:{request_id}"
    # The secret and the API token below are public values from the site's axios
    # interceptor. On 401/403, re-extract them by grepping
    # /_next/static/chunks/*.js for "x-api-signature".
    secret = b"LfmqpWYMaEuQA42LcDvmgbBgG4NDmZp73yr8G8pZ"
    digest = hmac.new(secret, message.encode(), "sha256").hexdigest()
    signature = b64encode(digest.encode()).decode()
    return {
        "x-api-token": "48a07687-2a14-4647-9d42-23d7f8ebfa45",
        "x-request-at": request_at,
        "x-request-id": request_id,
        "x-request-body": request_body,
        "x-api-signature": signature,
    }


def query_gql(
    context: Context, query: str, variables: dict[str, Any] | None = None
) -> Any:
    """Send a signed GraphQL request and return its data."""
    body = orjson.dumps({"query": query, "variables": variables})
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


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    period_start: str,
    period_end: str,
    member: dict[str, Any],
) -> None:
    member_data = member["anggota"]
    if member_data is None:
        # A few historical rows point to member records removed from the site.
        context.log.info(
            "Skipping roster record with missing member details",
            id_anggota=member["idAnggota"],
        )
        return
    member_id = member_data["id"]

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
        name = faction["fraksi"]["fraksi"]
        party = context.lookup_value("parties", name, name, warn_unmatched=True)
        person.add("political", party, lang="ind")
    # e.g. "Dr-H-C-PUAN-MAHARANI-287"
    slug = re.sub(r"[^A-Za-z0-9]+", "-", raw_name)
    person.add(
        "sourceUrl",
        "https://www.dpr.go.id/en/tentang-dpr/informasi-anggota-dewan/detail-anggota/"
        f"{slug}-{member_id}",
    )

    # Only a member who left the running term overrides the status: ended terms are
    # left to the period end (their statusOff is often blank), and make_occupancy
    # decides the rest.
    status = None
    if period_end >= str(settings.RUN_TIME.year):
        value = context.lookup_value(
            "occupancy_status", member["statusOff"], warn_unmatched=True
        )
        status = OccupancyStatus(value) if value is not None else None
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=period_start,
        period_end=period_end,
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

    # Trimmed from the site's own query. The page size is above the 580 seats plus
    # mid-term replacements, so one page holds a whole legislature.
    roster_query = """
    query ($periode: Mixed) {
      getDaftarRiwayatAnggota(
        first: 1000
        wherePeriode: { column: ID, operator: EQ, value: $periode }
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
    for periode in query_gql(context, "{ getAllPeriode { id data } }")["getAllPeriode"]:
        # Legislature labels, e.g. "Periode 2024 - 2029".
        match = re.fullmatch(
            r"(?:Periode\s+)?(\d{4})\s*-\s*(\d{4})", periode["data"].strip()
        )
        if match is None:
            raise ValueError(f"Cannot parse legislature period: {periode!r}")
        start, end = match.groups()
        if end < h.earliest_term_start(categorisation.topics):
            continue
        roster = query_gql(context, roster_query, {"periode": int(periode["id"])})
        for member in roster["getDaftarRiwayatAnggota"]["data"]:
            crawl_member(context, position, categorisation, start, end, member)

    assert_all_accepted(context, raise_on_unaccepted=False)
