import logging
from enum import Enum
from datetime import datetime
from functools import lru_cache
from importlib.metadata import metadata
from typing import Dict, List, Union
from pydantic import BaseModel, Field
from fastapi import FastAPI, Path, HTTPException
from starlette.responses import RedirectResponse
from followthemoney import model
from nomenklatura.index.index import Index
from nomenklatura.loader import Loader

from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset
from opensanctions.core.resolver import get_resolver
from opensanctions.core.loader import DatasetMemoryLoader

log = logging.getLogger(__name__)
meta = metadata("opensanctions")
app = FastAPI(
    title="OpenSanctions Matching API",
    version=meta["Version"],
    contact={
        "name": meta["Author"],
        "url": meta["Home-page"],
        "email": meta["Author-email"],
    },
)
dataset = Dataset.get("us_ofac_sdn")
resolver = get_resolver()


class EntityResponse(BaseModel):
    id: str
    schema_: str = Field("LegalEntity", alias="schema")
    properties: Dict[str, List[Union[str, "EntityResponse"]]]
    datasets: List[str]
    referents: List[str]
    first_seen: datetime
    last_seen: datetime


EntityResponse.update_forward_refs()


class SearchResponse(BaseModel):
    results: List[EntityResponse]


@lru_cache(maxsize=None)
def get_loader() -> Loader[Dataset, Entity]:
    if dataset is None:
        raise RuntimeError("Unkown dataset")
    log.info("Loading %s to in-memory cache..." % dataset)
    return DatasetMemoryLoader(dataset, resolver)


@lru_cache(maxsize=None)
def get_index(loader: Loader[Dataset, Entity]) -> Index[Dataset, Entity]:
    index = Index(loader)
    index.build()
    return index


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


@app.get("/search", response_model=SearchResponse)
async def search(
    q: str, schema: str = "LegalEntity", limit: int = 10, fuzzy: bool = False
):
    """Search matching entities based on a simple piece of text, e.g. a name."""
    loader = get_loader()
    index = get_index(loader)
    query = Entity(schema)
    query.add("name", q)
    query.add("notes", q)
    results = []
    for result, score in index.match_entities(query, limit=limit, fuzzy=fuzzy):
        result_data = result.to_nested_dict(loader)
        result_data["score"] = score
        results.append(result_data)

    return {"results": results}
