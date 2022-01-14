from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union
from followthemoney.model import ModelToDict
from pydantic import BaseModel, Field
from pydantic.networks import AnyHttpUrl

from opensanctions.core import Dataset
from osapi import settings

MAX_LIMIT = 1000
EntityProperties = Dict[str, List[Union[str, "EntityResponse"]]]


class EntityResponse(BaseModel):
    id: str = Field(..., example="NK-A7z....")
    caption: str = Field(..., example="John Doe")
    schema_: str = Field(..., example="LegalEntity", alias="schema")
    properties: EntityProperties = Field(..., example={"name": ["John Doe"]})
    datasets: List[str] = Field([], example=["us_ofac_sdn"])
    referents: List[str] = Field([], example=["ofac-1234"])
    first_seen: datetime = Field(..., example=datetime.utcnow())
    last_seen: datetime = Field(..., example=datetime.utcnow())


EntityResponse.update_forward_refs()


class ScoredEntityResponse(EntityResponse):
    score: float = 0.99
    match: bool = False


class IndexResponse(BaseModel):
    datasets: List[str] = Dataset.names()
    model: ModelToDict
    index: Dict[str, Any]


class HealthzResponse(BaseModel):
    status: str = "ok"


class SearchFacetItem(BaseModel):
    name: str
    label: str
    count: int = 1


class SearchFacet(BaseModel):
    label: str
    values: List[SearchFacetItem]


class SearchResponse(BaseModel):
    results: List[EntityResponse]
    facets: Dict[str, SearchFacet]
    limit: int
    offset: int = 0
    total: int = 0


class EntityExample(BaseModel):
    schema_: str = Field(..., example=settings.BASE_SCHEMA, alias="schema")
    properties: Dict[str, List[str]] = Field(..., example={"name": ["John Doe"]})


class EntityMatchQuery(BaseModel):
    queries: Dict[str, EntityExample]


class EntityMatches(BaseModel):
    results: List[ScoredEntityResponse]
    query: EntityExample


class EntityMatchResponse(BaseModel):
    responses: Dict[str, EntityMatches]


class StatementModel(BaseModel):
    id: str = Field(..., example="0000ad52d4d91a8...")
    entity_id: str = Field(..., example="ofac-1234")
    canonical_id: str = Field(..., example="NK-1234")
    prop: str = Field(..., example="alias")
    prop_type: str = Field(..., example="name")
    schema_: str = Field(..., example="LegalEntity", alias="schema")
    value: str = Field(..., example="John Doe")
    dataset: str = Field(..., example="default")
    target: bool = Field(..., example=True)
    unique: bool = Field(..., example=False)
    first_seen: datetime
    last_seen: datetime


class StatementResponse(BaseModel):
    results: List[StatementModel]
    limit: int
    offset: int = 0
    total: int = 0


class FreebaseType(BaseModel):
    id: str = Field(..., example="Person")
    name: str = Field(..., example="People")
    description: Optional[str] = Field(None, example="...")


class FreebaseProperty(BaseModel):
    id: str = Field(..., example="birthDate")
    name: str = Field(..., example="Date of birth")
    description: Optional[str] = Field(None, example="...")


class FreebaseEntity(BaseModel):
    id: str = Field(..., example="NK-A7z....")
    name: str = Field(..., example="John Doe")
    score: Optional[float] = Field(..., example=0.99)
    match: Optional[bool] = Field(..., example=False)
    description: Optional[str] = Field(None, example="...")
    type: List[FreebaseType]


class FreebaseResponse(BaseModel):
    code: str = "/api/status/ok"
    status: str = "200 OK"


class FreebaseSuggestResponse(FreebaseResponse):
    prefix: str


class FreebaseTypeSuggestResponse(FreebaseSuggestResponse):
    result: List[FreebaseType]


class FreebaseEntitySuggestResponse(FreebaseSuggestResponse):
    result: List[FreebaseEntity]


class FreebasePropertySuggestResponse(FreebaseSuggestResponse):
    result: List[FreebaseProperty]


class FreebaseManifestView(BaseModel):
    url: str


class FreebaseManifestSuggestType(BaseModel):
    service_url: AnyHttpUrl
    service_path: str


class FreebaseManifestSuggest(BaseModel):
    entity: FreebaseManifestSuggestType
    type: FreebaseManifestSuggestType
    property: FreebaseManifestSuggestType


class FreebaseManifest(BaseModel):
    versions: List[str] = Field(..., example=["0.2"])
    name: str = Field(..., example=settings.TITLE)
    identifierSpace: AnyHttpUrl
    schemaSpace: AnyHttpUrl
    view: FreebaseManifestView
    suggest: FreebaseManifestSuggest
    defaultTypes: List[FreebaseType]


class FreebaseEntityResult(BaseModel):
    result: List[FreebaseEntity]


FreebaseQueryResult = Dict[str, FreebaseEntityResult]
