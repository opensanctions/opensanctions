# Settings configuration for OpenSanctions
# Below settings can be configured via environment variables, which makes
# them easy to access in a dockerized environment.
from pathlib import Path
from os import environ as env

# Directory with metadata specifications for each crawler
METADATA_PATH = Path(__file__).resolve().parent.joinpath("metadata")
METADATA_PATH = Path(env.get("OPENSANCTIONS_METADATA_PATH", METADATA_PATH)).resolve()
