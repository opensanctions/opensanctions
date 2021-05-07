import requests
import structlog
import functools
from requests_cache.core import CachedSession

from opensanctions import settings

log = structlog.get_logger("http")
HEADERS = {"User-Agent": settings.USER_AGENT}


def get_session():
    """Make a cached session."""
    settings.CACHE_PATH.mkdir(exist_ok=True, parents=True)
    path = settings.CACHE_PATH.joinpath("http").as_posix()
    session = CachedSession(cache_name=path, expire_after=settings.CACHE_EXPIRE)
    session.headers.update(HEADERS)
    # weird monkey-patch: default timeout for requests sessions
    session.request = functools.partial(session.request, timeout=settings.HTTP_TIMEOUT)
    return session


def cleanup_cache():
    # Explicitly clear HTTP cache:
    session = get_session()
    session.cache.remove_expired_responses(expire_after=settings.CACHE_EXPIRE)


def fetch_download(file_path, url):
    """Circumvent the cache for large file downloads."""
    session = requests.Session()
    session.headers.update(HEADERS)
    log.info("Fetching artifact", path=file_path.as_posix(), url=url)
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with session.get(url, stream=True, timeout=settings.HTTP_TIMEOUT) as res:
        res.raise_for_status()
        with open(file_path, "wb") as handle:
            for chunk in res.iter_content(chunk_size=8192 * 16):
                handle.write(chunk)
