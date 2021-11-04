from datetime import datetime
from typing import Dict, List, Optional, TypedDict, Union
from pydantic import BaseModel, Field
from followthemoney.model import ModelToDict

from opensanctions.core import Dataset


class EntityResponse(BaseModel):
    id: str = "NK-A7z...."
    schema_: str = Field("LegalEntity", alias="schema")
    properties: Dict[str, List[Union[str, "EntityResponse"]]]
    datasets: List[str] = ["us_ofac_sdn"]
    referents: List[str] = ["ofac-1234"]
    first_seen: datetime = datetime.utcnow()
    last_seen: datetime = datetime.utcnow()


EntityResponse.update_forward_refs()


class IndexResponse(BaseModel):
    datasets: List[str] = Dataset.names()
    model: ModelToDict
    terms: int = 23
    tokens: int = 42


class HealthzResponse(BaseModel):
    status: str = "ok"


class SearchResponse(BaseModel):
    results: List[EntityResponse]


class FreebaseType(BaseModel):
    id: str = "Person"
    name: str = "People"
    description: str = "..."


class FreebaseProperty(BaseModel):
    id: str = "birthDate"
    name: str = "Date of birth"
    description: str = "..."


class FreebaseEntity(BaseModel):
    id: str = "NK-A7z...."
    name: str = "John Doe"
    score: Optional[float] = 0.99
    match: Optional[bool] = False
    description: Optional[str] = None
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
    service_url: str
    service_path: str


class FreebaseManifestSuggest(BaseModel):
    entity: FreebaseManifestSuggestType
    type: FreebaseManifestSuggestType
    property: FreebaseManifestSuggestType


class FreebaseManifest(BaseModel):
    versions: List[str] = ["0.2"]
    title: str
    identifierSpace: str
    schemaSpace: str
    view: FreebaseManifestView
    suggest: FreebaseManifestSuggest
    defaultTypes: List[FreebaseType]


class FreebaseEntityResult(BaseModel):
    result: List[FreebaseEntity]


FreebaseQueryResult = Dict[str, FreebaseEntityResult]
