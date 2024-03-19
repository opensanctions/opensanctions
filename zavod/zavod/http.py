import warnings
from typing import Any, Optional
from functools import partial
from pathlib import Path
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util import Retry

from zavod import settings
from zavod.logs import get_logger
from zavod.meta.http import HTTP

log = get_logger(__name__)
warnings.filterwarnings("ignore", category=InsecureRequestWarning)


def make_session(http_conf: HTTP) -> Session:
    session = Session()
    session.headers["User-Agent"] = http_conf.user_agent
    session.verify = False
    session.request = partial(  # type: ignore
        session.request,
        timeout=settings.HTTP_TIMEOUT,
    )
    retries = Retry(
        total=http_conf.total_retries,
        backoff_factor=http_conf.backoff_factor,
        status_forcelist=http_conf.retry_statuses,
        allowed_methods=http_conf.retry_methods,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def fetch_file(
    session: Session,
    url: str,
    name: str,
    data_path: Path = settings.DATA_PATH,
    auth: Optional[Any] = None,
    headers: Optional[Any] = None,
) -> Path:
    """Fetch a (large) file via HTTP to the data path."""
    out_path = data_path.joinpath(name)
    if out_path.exists():
        return out_path
    log.info("Fetching file", url=url)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with session.get(url, auth=auth, headers=headers, stream=True) as res:
        res.raise_for_status()
        with open(out_path, "wb") as fh:
            for chunk in res.iter_content(chunk_size=8192 * 10):
                fh.write(chunk)
    return out_path
