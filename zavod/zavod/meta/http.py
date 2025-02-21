from typing import Any, Dict, List

from urllib3.util import Retry
from banal import ensure_list
from zavod import settings

DEFAULT_RETRY_STATUS_CODES = [500, 502] + list(Retry.RETRY_AFTER_STATUS_CODES)


class HTTP(object):
    def __init__(self, data: Dict[str, Any]) -> None:
        self.total_retries: int = data.get("total_retries", settings.HTTP_RETRY_TOTAL)
        self.backoff_factor: float = data.get(
            "backoff_factor", settings.HTTP_RETRY_BACKOFF_FACTOR
        )
        statuses = ensure_list(data.get("retry_statuses", DEFAULT_RETRY_STATUS_CODES))
        self.backoff_max: int = settings.HTTP_RETRY_BACKOFF_MAX

        self.retry_statuses: List[int] = statuses
        retry_methods = ensure_list(
            data.get("retry_methods", list(Retry.DEFAULT_ALLOWED_METHODS))
        )
        self.retry_methods: List[str] = retry_methods
        self.user_agent: str = data.get("user_agent", settings.HTTP_USER_AGENT)
