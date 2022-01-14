# Settings configuration for OpenSanctions
# Below settings can be configured via environment variables, which makes
# them easy to access in a dockerized environment.
from pathlib import Path
from datetime import datetime
from os import environ as env
from normality import stringify


def env_str(name: str, default: str) -> str:
    """Ensure the env returns a string even on Windows (#100)."""
    value = stringify(env.get(name))
    return default if value is None else value


# Update the data every day (not totally trying to make this flexible yet,
# since a day seems like a pretty good universal rule).
INTERVAL = 84600

# HTTP cache expiry may last multiple runs
CACHE_EXPIRE = INTERVAL * 7

# All data storage (e.g. a Docker volume mount)
DATA_PATH = Path.cwd().joinpath("data")
DATA_PATH = Path(env.get("OPENSANCTIONS_DATA_PATH", DATA_PATH)).resolve()
DATA_PATH.mkdir(parents=True, exist_ok=True)

# Resources generated from specific datasets
DATASET_PATH = DATA_PATH.joinpath("datasets")
DATASET_PATH = Path(env.get("OPENSANCTIONS_DATASET_PATH", DATASET_PATH)).resolve()

# Public URL version
DATASET_URL = "https://data.opensanctions.org/datasets/latest/"
DATASET_URL = env_str("OPENSANCTIONS_DATASET_URL", DATASET_URL)

# Used to keep the HTTP cache
STATE_PATH_ = env.get("OPENSANCTIONS_STATE_PATH", DATA_PATH.joinpath("state"))
STATE_PATH = Path(STATE_PATH_).resolve()
STATE_PATH.mkdir(parents=True, exist_ok=True)

# SQL database URI for structured data
DATABASE_URI = env_str("OPENSANCTIONS_DATABASE_URI", None)
ASYNC_DATABASE_URI = DATABASE_URI.replace("postgresql://", "postgresql+asyncpg://")
DATABASE_POOL_SIZE = int(env_str("OPENSANCTIONS_POOL_SIZE", "30"))

# Per-run timestamp
RUN_TIME = datetime.utcnow().replace(microsecond=0)
RUN_DATE = RUN_TIME.date().isoformat()

# Directory with metadata specifications for each crawler
METADATA_PATH = Path(__file__).resolve().parent.joinpath("metadata")
METADATA_PATH = Path(env.get("OPENSANCTIONS_METADATA_PATH", METADATA_PATH)).resolve()

# Storage for static reference data
STATIC_PATH = Path(__file__).resolve().parent.joinpath("static")
STATIC_PATH = Path(env.get("OPENSANCTIONS_STATIC_PATH", STATIC_PATH)).resolve()

# Do not edit manually, use the release process
VERSION = "3.2.0"

# Relevance cut-off for dead people
DEATH_CUTOFF = datetime(2010, 1, 1)

# User agent
USER_AGENT = f"Mozilla/5.0 (any) OpenSanctions/{VERSION} info@opensanctions.org"
HTTP_TIMEOUT = 60

# If you change this, all bets are off
ENCODING = "utf-8"
