from datetime import datetime
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field
from followthemoney.model import ModelToDict
from pydantic.networks import AnyHttpUrl

from opensanctions.core import Dataset
from osapi import settings

EntityProperties = Dict[str, List[Union[str, "EntityResponse"]]]


class EntityResponse(BaseModel):
    id: str = Field(..., example="NK-A7z....")
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
    terms: int = Field(..., example=23)
    tokens: int = Field(..., example=42)


class HealthzResponse(BaseModel):
    status: str = "ok"


class SearchResponse(BaseModel):
    results: List[EntityResponse]


class EntityExample(BaseModel):
    schema_: str = Field(..., example=settings.BASE_SCHEMA, alias="schema")
    properties: Dict[str, List[Union[str]]]


class EntityMatchQuery(BaseModel):
    queries: Dict[str, EntityExample]


class EntityMatches(BaseModel):
    results: List[ScoredEntityResponse]
    query: EntityExample


class EntityMatchResponse(BaseModel):
    responses: Dict[str, EntityMatches]


class FreebaseType(BaseModel):
    id: str = Field(..., example="Person")
    name: str = Field(..., example="People")
    description: str = Field(..., example="...")


class FreebaseProperty(BaseModel):
    id: str = Field(..., example="birthDate")
    name: str = Field(..., example="Date of birth")
    description: str = Field(..., example="...")


class FreebaseEntity(BaseModel):
    id: str = Field(..., example="NK-A7z....")
    name: str = Field(..., example="John Doe")
    score: Optional[float] = Field(..., example=0.99)
    match: Optional[bool] = Field(..., example=False)
    description: Optional[str]
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
    title: str = Field(..., example=settings.TITLE)
    identifierSpace: AnyHttpUrl
    schemaSpace: AnyHttpUrl
    view: FreebaseManifestView
    suggest: FreebaseManifestSuggest
    defaultTypes: List[FreebaseType]


class FreebaseEntityResult(BaseModel):
    result: List[FreebaseEntity]


FreebaseQueryResult = Dict[str, FreebaseEntityResult]
