import uuid
import json

from os import environ as env
from zavod import Context

SE_SOE_API_CLIENT_ID = env["OPENSANCTIONS_SE_SOE_CLIENT_ID"]
SE_SOE_API_CLIENT_SECRET = env["OPENSANCTIONS_SE_SOE_CLIENT_SECRET"]


def get_access_token(context: Context) -> str:
    """Request OAuth2 access token using Client Credentials Grant (RFC 6749 Section 4.4)."""
    data = {
        "grant_type": "client_credentials",
        "scope": "vardefulla-datamangder:read",
    }
    token_data = context.fetch_json(
        "https://portal.api.bolagsverket.se/oauth2/token",
        auth=(SE_SOE_API_CLIENT_ID, SE_SOE_API_CLIENT_SECRET),
        data=data,
        method="POST",
        headers={"Accept": "application/json"},
    )
    return token_data["access_token"]


def crawl(context: Context) -> None:
    """Crawl Swedish government entities from Bolagsverket API."""
    token = get_access_token(context)
    headers = {
        "X-Request-Id": str(uuid.uuid4()),
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "accept": "application/json",
    }
    body = {"identitetsbeteckning": "2021001819"}

    _response = context.fetch_json(
        context.data_url,
        headers=headers,
        method="POST",
        data=json.dumps(body),
    )
