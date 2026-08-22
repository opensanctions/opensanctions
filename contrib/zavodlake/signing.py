"""Signed GCS URLs for reading the bucket directly, bypassing the CDN.

The archive bucket is not publicly readable — its public face is the
bunny.net CDN. For latency experiments against raw GCS we mint V4 signed
URLs instead. Local end-user credentials cannot sign, so signing goes
through the IAM signBlob API impersonating the read-only crawler-team
service account (crawler-team members hold serviceAccountTokenCreator
on it, see operations/tf/etl/service_account.tf).
"""

from datetime import timedelta

import google.auth
from google.auth.transport.requests import Request

from zavod.archive.backend import GoogleCloudBackend, get_archive_backend

SIGNER_EMAIL = "etl-crawlerteam-sa@opensanctions-ops.iam.gserviceaccount.com"
_token: str | None = None


def _access_token() -> str:
    global _token
    if _token is None:
        creds, _ = google.auth.default()
        creds.refresh(Request())  # type: ignore[no-untyped-call]
        assert creds.token is not None
        _token = str(creds.token)
    return _token


def signed_url(object_name: str, expire_hours: int = 6) -> str:
    """Mint a V4 signed GCS URL for an object in the archive bucket."""
    backend = get_archive_backend()
    assert isinstance(backend, GoogleCloudBackend), backend
    blob = backend.bucket.blob(object_name)
    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=expire_hours),
        service_account_email=SIGNER_EMAIL,
        access_token=_access_token(),
    )
    return str(url)
