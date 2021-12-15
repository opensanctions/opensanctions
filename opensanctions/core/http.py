# import urllib3
import requests
import warnings
import structlog
from functools import cache, partial
from requests_cache import CachedSession

from opensanctions import settings

log = structlog.get_logger("http")
HEADERS = {"User-Agent": settings.USER_AGENT}

warnings.simplefilter("ignore", category=ResourceWarning)

# cf. https://stackoverflow.com/questions/38015537 for pace.coe.int
requests.packages.urllib3.disable_warnings()
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"


@cache
def get_session() -> CachedSession:
    """Make a cached session."""
    path = settings.STATE_PATH.joinpath("http").as_posix()
    session = CachedSession(cache_name=path, expire_after=settings.CACHE_EXPIRE)
    session.headers.update(HEADERS)
    # weird monkey-patch: default timeout for requests sessions
    session.request = partial(session.request, timeout=settings.HTTP_TIMEOUT)
    return session


def cleanup_cache():
    # Explicitly clear HTTP cache:
    session = get_session()
    session.cache.remove_expired_responses(expire_after=settings.CACHE_EXPIRE)


def fetch_download(file_path, url: str):
    """Circumvent the cache for large file downloads."""
    session = requests.Session()
    session.headers.update(HEADERS)
    log.info("Fetching resource", path=file_path.as_posix(), url=url)
    file_path.parent.mkdir(exist_ok=True, parents=True)
    with session.get(url, stream=True, timeout=settings.HTTP_TIMEOUT) as res:
        res.raise_for_status()
        with open(file_path, "wb") as handle:
            for chunk in res.iter_content(chunk_size=8192 * 16):
                handle.write(chunk)
