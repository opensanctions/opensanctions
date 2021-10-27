import json
import logging
from urllib.parse import urljoin
from typing import Optional
from fastapi import FastAPI, Path, Query, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from followthemoney.types import registry
from starlette.responses import RedirectResponse
from followthemoney import model
from followthemoney.exc import InvalidData
from api.osapi.data import (
    get_freebase_entity,
    get_freebase_property,
    get_matchable_schemata,
)
from opensanctions.core.entity import Entity
from opensanctions.core.logs import configure_logging

from osapi import settings
from osapi.models import EntityResponse, SearchResponse
from osapi.data import dataset, resolver
from osapi.data import get_loader, get_index, get_schemata
from osapi.data import get_freebase_type, get_freebase_types
from osapi.util import match_prefix

log = logging.getLogger(__name__)
app = FastAPI(
    title="OpenSanctions Matching API",
    version=settings.VERSION,
    contact=settings.CONTACT,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
configure_logging(level=logging.INFO)


@app.on_event("startup")
async def startup_event():
    loader = get_loader()
    get_index(loader)


@app.get("/")
async def index():
    """Get system configuration information."""
    loader = get_loader()
    index = get_index(loader)
    return {
        "dataset": dataset.to_dict(),
        "model": model.to_dict(),
        "index": {"terms": len(index.terms), "tokens": len(index.inverted)},
    }


@app.get("/healthz")
async def healthz():
    """No-op basic health check."""
    return {"status": "ok"}


@app.get("/entities/{entity_id}", response_model=EntityResponse)
async def get_entity(
    entity_id: str = Path(None, title="The ID of the entity to retrieve")
):
    """Retrieve a single entity by its ID."""
    loader = get_loader()
    canonical_id = resolver.get_canonical(entity_id)
    if canonical_id != entity_id:
        url = app.url_path_for("get_entity", entity_id=canonical_id)
        return RedirectResponse(url=url)
    entity = loader.get_entity(entity_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="No such entity!")
    return entity.to_nested_dict(loader)


@app.get("/search/{dataset}", response_model=SearchResponse)
async def search(
    q: str,
    dataset: str = Path(dataset.name, title="Data source or collection ID"),
    schema: str = Query(settings.BASE_SCHEMA, title="Types of entities that can match"),
    limit: int = Query(10, title="Number of results to return"),
    fuzzy: bool = Query(False, title="Enable n-gram matching of partial names"),
    nested: bool = Query(False, title="Include adjacent entities in response"),
):
    """Search matching entities based on a simple piece of text, e.g. a name."""
    loader = get_loader()
    index = get_index(loader)
    query = Entity(schema)
    query.add("name", q)
    query.add("notes", q)
    results = []
    for result, score in index.match_entities(query, limit=limit, fuzzy=fuzzy):
        result_data = None
        if nested:
            result_data = result.to_nested_dict(loader)
        else:
            result_data = result.to_dict()
        result_data["score"] = score
        results.append(result_data)
    return {"results": results}


@app.get("/reconcile/{dataset}")
def reconcile(
    queries: Optional[str] = None,
    dataset: str = Path(dataset.name, title="Data source or collection ID"),
):
    """Reconciliation API, emulates Google Refine API. This endpoint can be used
    to bulk match entities against the system using an end-user application like
    [OpenRefine](https://openrefine.org).

    See: [Reconciliation API docs](https://reconciliation-api.github.io/specs/latest/#structure-of-a-reconciliation-query)
    """
    if queries is not None:
        return reconcile_queries(queries)
    base_url = urljoin(settings.ENDPOINT_URL, f"/reconcile/{dataset}")
    return {
        "versions": ["0.2"],
        "name": f"{dataset.title} ({app.title})",
        "identifierSpace": "https://opensanctions.org/reference/#schema",
        "schemaSpace": "https://opensanctions.org/reference/#schema",
        "view": {"url": ("https://opensanctions.org/entities/{{id}}/")},
        "suggest": {
            "entity": {
                "service_url": base_url,
                "service_path": "/suggest/entity",
            },
            "type": {
                "service_url": base_url,
                "service_path": "/suggest/type",
            },
            "property": {
                "service_url": base_url,
                "service_path": "/suggest/property",
            },
        },
        "defaultTypes": get_freebase_types(),
    }


@app.post("/reconcile")
def reconcile_post(queries: str = Form("")):
    """Reconciliation API, emulates Google Refine API."""
    return reconcile_queries(queries)


def reconcile_queries(queries):
    # multiple requests in one query
    try:
        queries = json.loads(queries)
    except ValueError:
        raise HTTPException(status_code=400, detail="Cannot decode query")
    results = {}
    for k, q in queries.items():
        results[k] = reconcile_query(q)
    # log.info("RESULTS: %r" % results)
    return results


def reconcile_query(query):
    """Reconcile operation for a single query."""
    # log.info("Reconcile: %r", query)
    limit = int(query.get("limit", 5))
    type = query.get("type", settings.BASE_SCHEMA)
    loader = get_loader()
    index = get_index(loader)
    proxy = Entity(type)
    proxy.add("name", query.get("query"))
    proxy.add("notes", query.get("query"))
    for p in query.get("properties", []):
        prop = model.get_qname(p.get("pid"))
        if prop is None:
            continue
        try:
            proxy.add_cast(prop.schema, prop.name, p.get("v"), fuzzy=True)
        except InvalidData:
            log.exception("Invalid property is set.")

    results = []
    # log.info("QUERY %r %s", proxy.to_dict(), limit)
    for result, score in index.match_entities(proxy, limit=limit, fuzzy=True):
        results.append(get_freebase_entity(result, score))
    return {"result": results}


@app.get("/reconcile/suggest/entity")
def reconcile_suggest_entity(prefix: str = "", limit: int = 10):
    """Suggest an entity API, emulates Google Refine API.

    This is functionally very similar to the basic search API, but returns
    data in the structure assumed by the
    [Reconciliation API](https://reconciliation-api.github.io/specs/latest/#suggest-services).

    Searches are conducted based on name and text content, using all matchable
    entities in the system index."""
    loader = get_loader()
    index = get_index(loader)
    query = Entity(settings.BASE_SCHEMA)
    query.add("name", prefix)
    query.add("notes", prefix)
    results = []
    for result, score in index.match_entities(query, limit=limit, fuzzy=True):
        results.append(get_freebase_entity(result, score))
    return {
        "code": "/api/status/ok",
        "status": "200 OK",
        "prefix": prefix,
        "result": results,
    }


@app.get("/reconcile/suggest/property")
def reconcile_suggest_property(prefix: str = ""):
    """Given a search prefix, return all the type/schema properties which match
    the given text. This is used to auto-complete property selection for detail
    filters in OpenRefine."""
    matches = []
    for prop in model.properties:
        if not prop.schema.is_a(settings.BASE_SCHEMA):
            continue
        if prop.hidden or prop.type == prop.type == registry.entity:
            continue
        if match_prefix(prefix, prop.name, prop.label):
            matches.append(get_freebase_property(prop))
    return {
        "code": "/api/status/ok",
        "status": "200 OK",
        "prefix": prefix,
        "result": matches,
    }


@app.get("/reconcile/suggest/type")
def suggest_type(prefix: str = ""):
    """Given a search prefix, return all the types (i.e. schema) which match
    the given text. This is used to auto-complete type selection for the
    configuration of reconciliation in OpenRefine."""
    matches = []
    for schema in get_matchable_schemata():
        if match_prefix(prefix, schema.name, schema.label):
            matches.append(get_freebase_type(schema))
    return {
        "code": "/api/status/ok",
        "status": "200 OK",
        "prefix": prefix,
        "result": matches,
    }
