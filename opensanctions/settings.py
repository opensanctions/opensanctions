# Settings configuration for OpenSanctions
# Below settings can be configured via environment variables, which makes
# them easy to access in a dockerized environment.
from pathlib import Path
from datetime import datetime
from os import environ as env
from normality import stringify

from nomenklatura import db


def env_str(name: str, default: str) -> str:
    """Ensure the env returns a string even on Windows (#100)."""
    value = stringify(env.get(name))
    return default if value is None else value


# All data storage (e.g. a Docker volume mount)
DATA_PATH = Path.cwd().joinpath("data")
DATA_PATH = Path(env.get("OPENSANCTIONS_DATA_PATH", DATA_PATH)).resolve()
DATA_PATH.mkdir(parents=True, exist_ok=True)

# Resources generated from specific datasets
DATASET_PATH = DATA_PATH.joinpath("datasets")
DATASET_PATH = Path(env.get("OPENSANCTIONS_DATASET_PATH", DATASET_PATH)).resolve()

# Bucket to back-fill missing data artifacts from
BACKFILL_BUCKET = env.get("OPENSANCTIONS_BACKFILL_BUCKET", None)
BACKFILL_VERSION = env_str("OPENSANCTIONS_BACKFILL_VERSION", "latest")

# SQL database URI for structured data
DATABASE_URI = env.get("OPENSANCTIONS_DATABASE_URI")
if DATABASE_URI is None:
    raise RuntimeError("Please set $OPENSANCTIONS_DATABASE_URI.")
if not DATABASE_URI.startswith("postgres"):
    raise RuntimeError("Unsupported database engine: %s" % DATABASE_URI)
db.DB_URL = DATABASE_URI
db.POOL_SIZE = int(env_str("OPENSANCTIONS_POOL_SIZE", db.POOL_SIZE))

# Per-run timestamp
RUN_TIME = datetime.utcnow().replace(microsecond=0)
RUN_TIME_ISO = RUN_TIME.isoformat(sep="T", timespec="seconds")
RUN_DATE = RUN_TIME.date().isoformat()

# Directory with metadata specifications for each crawler
METADATA_PATH = Path(__file__).resolve().parent.joinpath("metadata")
METADATA_PATH = Path(env.get("OPENSANCTIONS_METADATA_PATH", METADATA_PATH)).resolve()

# Storage for static reference data
STATIC_PATH = Path(__file__).resolve().parent.joinpath("static")
STATIC_PATH = Path(env.get("OPENSANCTIONS_STATIC_PATH", STATIC_PATH)).resolve()

# Resolver file path
RESOLVER_PATH = env.get("OPENSANCTIONS_RESOLVER_PATH")
if RESOLVER_PATH is None:
    raise RuntimeError("Please set $OPENSANCTIONS_RESOLVER_PATH.")

# User agent
USER_AGENT = "Mozilla/5.0 (any)"
USER_AGENT = env_str("OPENSANCTIONS_USER_AGENT", USER_AGENT)
HEADERS = {"User-Agent": USER_AGENT}
HTTP_TIMEOUT = 240

# If you change this, all bets are off
ENCODING = "utf-8"
