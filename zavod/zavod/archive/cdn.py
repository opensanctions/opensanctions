import requests
from rigour.env import env_str

from zavod import settings
from zavod.logs import get_logger

log = get_logger(__name__)


def invalidate_archive_cache(path: str) -> None:
    """Invalidate the archive CDN cache. This is a deployment-specific operation for
    OpenSanctions corporate and not needed for operators who don't serve the archive
    via a bunny.net CDN."""
    bunnynet_api_key = env_str("BUNNYNET_API_KEY", "").strip()
    if not len(bunnynet_api_key):
        log.debug("$BUNNYNET_API_KEY not set, skipping CDN invalidation")
        return

    purge_url = "https://api.bunny.net/purge"
    headers = {"AccessKey": bunnynet_api_key}
    archive_url = f"{settings.ARCHIVE_SITE}/{path}"
    params = {"url": archive_url, "async": "true"}

    try:
        response = requests.post(purge_url, headers=headers, params=params)
        response.raise_for_status()
        log.info("Invalidated archive CDN cache: %s" % path)
    except requests.RequestException as e:
        log.error("Failed to invalidate archive CDN cache", path=path, error=str(e))
