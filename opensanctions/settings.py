# Settings configuration for OpenSanctions
# Below settings can be configured via environment variables, which makes
# them easy to access in a dockerized environment.
from pathlib import Path
from datetime import datetime
from os import environ as env

# Update the data every day (not totally trying to make this flexible yet,
# since a day seems like a pretty good universal rule).
INTERVAL = 84600

# HTTP cache expiry may last multiple runs
CACHE_EXPIRE = INTERVAL * 7

# All data storage (e.g. a Docker volume mount)
DATA_PATH = Path.cwd().joinpath("data")
DATA_PATH = env.get("OPENSANCTIONS_DATA_PATH", DATA_PATH)
DATA_PATH = Path(DATA_PATH).resolve()

# Artifacts generated from specific datasets
DATASET_PATH = DATA_PATH.joinpath("datasets")
DATASET_PATH = env.get("OPENSANCTIONS_DATASET_PATH", DATASET_PATH)
DATASET_PATH = Path(DATASET_PATH).resolve()

# Public URL version
DATASET_URL = "https://data.opensanctions.org/datasets/latest/"
DATASET_URL = env.get("OPENSANCTIONS_DATASET_URL", DATASET_URL)

# Used to keep the HTTP cache
CACHE_PATH = DATA_PATH.joinpath("cache")
CACHE_PATH = env.get("OPENSANCTIONS_CACHE_PATH", CACHE_PATH)
CACHE_PATH = Path(CACHE_PATH).resolve()

# PostgreSQL database URI for structured data
DATABASE_URI = env.get("OPENSANCTIONS_DATABASE_URI")

# Per-run timestamp
RUN_TIME = datetime.utcnow().replace(microsecond=0)
RUN_DATE = RUN_TIME.date().isoformat()

# Directory with metadata specifications for each crawler
METADATA_PATH = Path(__file__).resolve().parent.joinpath("metadata")
METADATA_PATH = env.get("OPENSANCTIONS_METADATA_PATH", METADATA_PATH)
METADATA_PATH = Path(METADATA_PATH).resolve()

# User agent
USER_AGENT = "Mozilla/5.0 (any) OpenSanctions/3"
HTTP_TIMEOUT = 60
