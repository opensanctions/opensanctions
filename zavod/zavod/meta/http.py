import logging
from banal import ensure_list
from urllib3.util import Retry
from typing import Any, Dict, List, Set
from followthemoney.settings import USER_AGENT

from zavod import settings


DEFAULT_RETRY_STATUS_CODES = [500, 502, 504] + list(Retry.RETRY_AFTER_STATUS_CODES)


class HTTP(object):
    def __init__(self, data: Dict[str, Any]) -> None:
        self.total_retries: int = data.get("total_retries", settings.HTTP_RETRY_TOTAL)
        self.backoff_factor: float = data.get(
            "backoff_factor", settings.HTTP_RETRY_BACKOFF_FACTOR
        )
        statuses: Set[int] = set(
            data.get("retry_statuses", DEFAULT_RETRY_STATUS_CODES)
            + data.get("additional_retry_statuses", [])
        )
        if "additional_retry_statuses" in data and "retry_statuses" in data:
            logging.warning(
                "Both 'retry_statuses' and 'additional_retry_statuses' are set."
            )

        self.backoff_max: int = settings.HTTP_RETRY_BACKOFF_MAX
        self.retry_statuses: List[int] = list(statuses)
        retry_methods: List[str] = ensure_list(
            data.get("retry_methods", list(Retry.DEFAULT_ALLOWED_METHODS))
        )
        self.retry_methods: List[str] = retry_methods
        self.user_agent: str = data.get("user_agent", USER_AGENT)
