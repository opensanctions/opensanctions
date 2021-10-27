import json
import logging
from urllib.parse import urljoin
from typing import Any, Dict, Optional
from fastapi import FastAPI, Path, Query, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from followthemoney.types import registry
from starlette.responses import RedirectResponse
from followthemoney import model
from followthemoney.exc import InvalidData
from api.osapi.models import (
    FreebaseEntitySuggestResponse,
    FreebasePropertySuggestResponse,
    FreebaseTypeSuggestResponse,
    HealthzResponse,
    IndexResponse,
)
from opensanctions.model import db
from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.core.logs import configure_logging

from osapi import settings
from osapi.models import EntityResponse, SearchResponse
from osapi.data import get_entity, get_index, resolver
from osapi.data import get_loader, match_entities, get_datasets
from osapi.data import get_freebase_type, get_freebase_types
from osapi.data import get_freebase_entity, get_freebase_property
from osapi.data import get_matchable_schemata, get_scope

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

PATH_DATASET = Path(
    settings.SCOPE_DATASET,
    description="Data source or collection name",
    example=settings.SCOPE_DATASET,
)
QUERY_PREFIX = Query(None, min_length=1, description="Search prefix")


@app.on_event("startup")
async def startup_event():
    get_index()
    db.session.close()


def get_dataset(name: str) -> Dataset:
    dataset = Dataset.get(name)
    if dataset is None or dataset not in get_datasets():
        raise HTTPException(status_code=404, detail="No such dataset.")
    return dataset


@app.get("/", response_model=IndexResponse)
async def index():
    """Get system configuration information."""
    try:
        index = get_index()
        return {
            "datasets": [ds.name for ds in get_datasets()],
            "model": model.to_dict(),
            "terms": len(index.terms),
            "tokens": len(index.inverted),
        }
    finally:
        db.session.close()


@app.get("/healthz", response_model=HealthzResponse)
async def healthz():
    """No-op basic health check."""
    try:
        return {"status": "ok"}
    finally:
        db.session.close()


@app.get("/entities/{entity_id}", response_model=EntityResponse)
async def fetch_entity(
    entity_id: str = Path(None, description="ID of the entity to retrieve")
):
    """Retrieve a single entity by its ID."""
    try:
        canonical_id = resolver.get_canonical(entity_id)
        if canonical_id != entity_id:
            url = app.url_path_for("get_entity", entity_id=canonical_id)
            return RedirectResponse(url=url)
        scope = get_scope()
        loader = get_loader(scope)
        entity = get_entity(scope, entity_id)
        if entity is None:
            raise HTTPException(status_code=404, detail="No such entity!")
        return entity.to_nested_dict(loader)
    finally:
        db.session.close()


@app.get("/search/{dataset}", response_model=SearchResponse)
async def search(
    q: str,
    dataset: str = PATH_DATASET,
    schema: str = Query(settings.BASE_SCHEMA, title="Types of entities that can match"),
    limit: int = Query(10, title="Number of results to return"),
    fuzzy: bool = Query(False, title="Enable n-gram matching of partial names"),
    nested: bool = Query(False, title="Include adjacent entities in response"),
):
    """Search matching entities based on a simple piece of text, e.g. a name."""
    try:
        ds = get_dataset(dataset)
        query = Entity(schema)
        query.add("name", q)
        query.add("notes", q)
        results = []
        loader = get_loader(ds)
        for result, score in match_entities(ds, query, limit=limit, fuzzy=fuzzy):
            result_data = None
            if nested:
                result_data = result.to_nested_dict(loader)
            else:
                result_data = result.to_dict()
            result_data["score"] = score
            results.append(result_data)
        return {"results": results}
    finally:
        db.session.close()


@app.get("/reconcile/{dataset}")
def reconcile(
    queries: Optional[str] = None,
    dataset: str = PATH_DATASET,
):
    """Reconciliation API, emulates Google Refine API. This endpoint can be used
    to bulk match entities against the system using an end-user application like
    [OpenRefine](https://openrefine.org).

    See: [Reconciliation API docs](https://reconciliation-api.github.io/specs/latest/#structure-of-a-reconciliation-query)
    """
    try:
        ds = get_dataset(dataset)
        if queries is not None:
            return reconcile_queries(ds, queries)
        base_url = urljoin(settings.ENDPOINT_URL, f"/reconcile/{dataset}")
        return {
            "versions": ["0.2"],
            "name": f"{ds.title} ({app.title})",
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
            "defaultTypes": get_freebase_types(ds),
        }
    finally:
        db.session.close()


@app.post("/reconcile/{dataset}")
def reconcile_post(
    dataset: str = PATH_DATASET,
    queries: str = Form(None, description="JSON-encoded reconciliation queries"),
):
    """Reconciliation API, emulates Google Refine API."""
    try:
        ds = get_dataset(dataset)
        return reconcile_queries(ds, queries)
    finally:
        db.session.close()


def reconcile_queries(dataset: Dataset, queries: str):
    # multiple requests in one query
    try:
        queries = json.loads(queries)
        results = {}
        for k, q in queries.items():
            results[k] = reconcile_query(dataset, q)
        # log.info("RESULTS: %r" % results)
        return results
    except ValueError:
        raise HTTPException(status_code=400, detail="Cannot decode query")


def reconcile_query(dataset: Dataset, query: Dict[str, Any]):
    """Reconcile operation for a single query."""
    # log.info("Reconcile: %r", query)
    limit = int(query.get("limit", 5))
    type = query.get("type", settings.BASE_SCHEMA)
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
    for result, score in match_entities(dataset, proxy, limit=limit, fuzzy=True):
        results.append(get_freebase_entity(result, score))
    return {"result": results}


@app.get(
    "/reconcile/{dataset}/suggest/entity",
    response_model=FreebaseEntitySuggestResponse,
)
def reconcile_suggest_entity(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
    limit: int = Query(10, description="Number of suggestions to return"),
):
    """Suggest an entity API, emulates Google Refine API.

    This is functionally very similar to the basic search API, but returns
    data in the structure assumed by the
    [Reconciliation API](https://reconciliation-api.github.io/specs/latest/#suggest-services).

    Searches are conducted based on name and text content, using all matchable
    entities in the system index."""
    try:
        ds = get_dataset(dataset)
        query = Entity(settings.BASE_SCHEMA)
        query.add("name", prefix)
        query.add("notes", prefix)
        results = []
        for result, score in match_entities(ds, query, limit=limit, fuzzy=True):
            results.append(get_freebase_entity(result, score))
        return {
            "code": "/api/status/ok",
            "status": "200 OK",
            "prefix": prefix,
            "result": results,
        }
    finally:
        db.session.close()


@app.get(
    "/reconcile/{dataset}/suggest/property",
    response_model=FreebasePropertySuggestResponse,
)
def reconcile_suggest_property(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
):
    """Given a search prefix, return all the type/schema properties which match
    the given text. This is used to auto-complete property selection for detail
    filters in OpenRefine."""
    try:
        ds = get_dataset(dataset)
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
    finally:
        db.session.close()


@app.get(
    "/reconcile/{dataset}/suggest/type",
    response_model=FreebaseTypeSuggestResponse,
)
def suggest_type(
    dataset: str = PATH_DATASET,
    prefix: str = QUERY_PREFIX,
):
    """Given a search prefix, return all the types (i.e. schema) which match
    the given text. This is used to auto-complete type selection for the
    configuration of reconciliation in OpenRefine."""
    try:
        ds = get_dataset(dataset)
        matches = []
        for schema in get_matchable_schemata(ds):
            if match_prefix(prefix, schema.name, schema.label):
                matches.append(get_freebase_type(schema))
        return {
            "code": "/api/status/ok",
            "status": "200 OK",
            "prefix": prefix,
            "result": matches,
        }
    finally:
        db.session.close()
