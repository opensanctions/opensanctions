import requests
import structlog
from requests_cache.core import CachedSession

from opensanctions import settings

log = structlog.get_logger("http")


def get_session():
    """Make a cached session."""
    path = settings.DATA_PATH.joinpath("http").as_posix()
    expire = int(settings.INTERVAL * 0.7)
    session = CachedSession(cache_name=path, expire_after=expire)
    session.headers["User-Agent"] = "OpenSanctions/3"
    return session


def fetch_download(file_path, url):
    """Circumvent the cache for large file downloads."""
    log.info("Fetching artifact", path=file_path.as_posix(), url=url)
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with requests.get(url, stream=True) as res:
        res.raise_for_status()
        with open(file_path, "wb") as handle:
            for chunk in res.iter_content(chunk_size=8192):
                handle.write(chunk)