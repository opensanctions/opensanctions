from banal import as_bool
from importlib.metadata import metadata
from opensanctions.settings import env_str

meta = metadata("opensanctions")
VERSION = meta["Version"]
AUTHOR = meta["Author"]
HOME_PAGE = meta["Home-page"]
EMAIL = meta["Author-email"]
CONTACT = {"name": AUTHOR, "url": HOME_PAGE, "email": EMAIL}

TITLE = "OpenSanctions Matching API"
DESCRIPTION = """
The OpenSanctions API provides tools that help you determine if any of the people
or companies mentioned in your data are subject to international sanctions, known
to be involved in criminal activity, or if they are politically exposed people.

**IMPORTANT: This open source API is intended to be operated on-premises in your
infrastructure. The online version exists as a demo and does not provide any data
protection or uptime guarantees. Read below on deploying your own instance.**

In this service, there is support for the following operations:

* A simple text-based search for interactive applications (``/search``),
* A query-by-example endpoint for KYC-style tasks (``/match``),
* Support for getting graph data for a particular entity (``/entities``),
* Support for the OpenRefine Reconciliation API (``/reconcile``).

The API uses JSON for data transfer and does not support authentication or access
control.

Further reading:

* [Install and deployment](https://github.com/pudo/opensanctions/blob/main/api/README.md)
* [Data dictionary](https://opensanctions.org/reference/)
"""

TAGS = [
    {
        "name": "Matching",
        "description": "Services that enable driving a user-facing entity search or"
        "matching a local data store against the given dataset.",
        "externalDocs": {
            "description": "Data dictionary",
            "url": "https://opensanctions.org/reference/",
        },
    },
    {
        "name": "System information",
        "description": "Service metadata endpoints for health checking and getting "
        "the application metadata to be used in client applications.",
    },
    {
        "name": "Reconciliation",
        "description": "The Reconciliation Service provides four separate endpoints"
        "that work in concert to implement the data matching API used by OpenRefine, "
        "Wikidata and several other services and utilities. Point ",
        "externalDocs": {
            "description": "Community specification",
            "url": "https://reconciliation-api.github.io/specs/latest/",
        },
    },
]

SCOPE_DATASET = env_str("OSAPI_SCOPE_DATASET", "default")
ENDPOINT_URL = env_str("OSAPI_ENDPOINT_URL", "http://localhost:8000")
ES_URL = env_str("OSAPI_ELASTICSEARCH_URL", "http://localhost:9200")
ES_INDEX = env_str("OSAPI_ELASTICSEARCH_INDEX", "opensanctions-api")
BASE_SCHEMA = "Thing"
