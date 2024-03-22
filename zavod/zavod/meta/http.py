from typing import Any, Dict, List
from urllib3.util import Retry
from banal import ensure_list
from zavod import settings


class HTTP(object):
    def __init__(self, data: Dict[str, Any]) -> None:
        self.total_retries: int = data.get("total_retries", 3)
        self.backoff_factor: float = data.get("backoff_factor", 1)
        statuses = ensure_list(
            data.get("retry_statuses", list(Retry.RETRY_AFTER_STATUS_CODES))
        )
        self.retry_statuses: List[int] = statuses
        retry_methods = ensure_list(
            data.get("retry_methods", list(Retry.DEFAULT_ALLOWED_METHODS))
        )
        self.retry_methods: List[str] = retry_methods
        self.user_agent: str = data.get("user_agent", settings.HTTP_USER_AGENT)
