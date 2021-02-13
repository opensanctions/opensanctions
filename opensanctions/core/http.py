import requests
import structlog
from requests_cache.core import CachedSession

from opensanctions import settings

log = structlog.get_logger("http")
HEADERS = {"User-Agent": settings.USER_AGENT}


def get_session(path):
    """Make a cached session."""
    path.mkdir(exist_ok=True, parents=True)
    path = path.joinpath("http").as_posix()
    expire = int(settings.INTERVAL * 0.7)
    session = CachedSession(cache_name=path, expire_after=expire)
    session.headers.update(HEADERS)
    return session


def fetch_download(file_path, url):
    """Circumvent the cache for large file downloads."""
    session = requests.Session()
    session.headers.update(HEADERS)
    log.info("Fetching artifact", path=file_path.as_posix(), url=url)
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with session.get(url, stream=True, timeout=30) as res:
        res.raise_for_status()
        with open(file_path, "wb") as handle:
            for chunk in res.iter_content(chunk_size=8192 * 10):
                handle.write(chunk)