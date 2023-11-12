from os import environ as env
from pathlib import Path
from banal import as_bool
from datetime import datetime
from normality import stringify


def env_str(name: str, default: str) -> str:
    """Ensure the env returns a string even on Windows (#100)."""
    value = stringify(env.get(name))
    return default if value is None else value


# Logging configuration
LOG_JSON = as_bool(env_str("ZAVOD_LOG_JSON", "false"))

# Debug mode
DEBUG = as_bool(env_str("ZAVOD_DEBUG", "false"))

# Default paths
DATA_PATH_ = env_str("ZAVOD_DATA_PATH", "data")
DATA_PATH = Path(env_str("OPENSANCTIONS_DATA_PATH", DATA_PATH_)).resolve()

# Per-run timestamp
RUN_TIME = datetime.utcnow().replace(microsecond=0)
RUN_TIME_ISO = RUN_TIME.isoformat(sep="T", timespec="seconds")
RUN_DATE = RUN_TIME.date().isoformat()

# Release version
RELEASE = env_str("ZAVOD_RELEASE", RUN_TIME.strftime("%Y%m%d"))

# Public URL version
PUBLIC_DOMAIN = env_str("ZAVOD_PUBLIC_DOMAIN", "data.opensanctions.org")
DATASET_URL = f"https://{PUBLIC_DOMAIN}/datasets/{RELEASE}/"
DATASET_URL = env_str("ZAVOD_DATASET_URL", DATASET_URL)

# Bucket to back-fill missing data artifacts from
ARCHIVE_BACKEND = env.get("ZAVOD_ARCHIVE_BACKEND", "FileSystemBackend")
ARCHIVE_BUCKET = env.get("ZAVOD_ARCHIVE_BUCKET", None)
ARCHIVE_BUCKET = env.get("OPENSANCTIONS_BACKFILL_BUCKET", ARCHIVE_BUCKET)
ARCHIVE_PATH = Path(env.get("ZAVOD_ARCHIVE_PATH", DATA_PATH.joinpath("archive")))
BACKFILL_RELEASE = env_str("ZAVOD_BACKFILL_RELEASE", "latest")

# File path for the resolver path used for entity deduplication
RESOLVER_PATH = env.get("ZAVOD_RESOLVER_PATH")
RESOLVER_PATH = env.get("OPENSANCTIONS_RESOLVER_PATH", RESOLVER_PATH)

# HTTP settings
HTTP_TIMEOUT = 1200
HTTP_USER_AGENT = "Mozilla/5.0 (zavod)"
HTTP_USER_AGENT = env_str("ZAVOD_HTTP_USER_AGENT", HTTP_USER_AGENT)

# Database-backed cache settings
CACHE_DATABASE_URI = env.get("ZAVOD_DATABASE_URI")
CACHE_DATABASE_URI = env.get("OPENSANCTIONS_DATABASE_URI", CACHE_DATABASE_URI)

# Load DB batch size
DB_BATCH_SIZE = int(env_str("ZAVOD_DB_BATCH_SIZE", "10000"))

OPENSANCTIONS_API_URL = env.get("ZAVOD_OPENSANCTIONS_API_URL", "https://api.opensanctions.org")
OPENSANCTIONS_API_KEY = env.get("ZAVOD_OPENSANCTIONS_API_KEY", None)