from datetime import datetime
from typing import Dict, List, Optional, TypedDict, Union
from pydantic import BaseModel, Field


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


class FreebaseType(TypedDict):
    id: str
    name: str
    description: str


class FreebaseProperty(TypedDict):
    id: str
    name: str
    description: str


class FreebaseEntity(TypedDict):
    id: str
    name: str
    score: Optional[float]
    match: Optional[bool]
    description: Optional[str]
    type: List[FreebaseType]


class FreebaseResponse(TypedDict):
    code: str
    status: str


class FreebaseSuggestResponse(FreebaseResponse):
    prefix: str


class FreebaseTypeSuggestResponse(FreebaseSuggestResponse):
    result: List[FreebaseType]


class FreebaseEntitySuggestResponse(FreebaseSuggestResponse):
    result: List[FreebaseEntity]


class FreebasePropertySuggestResponse(FreebaseSuggestResponse):
    result: List[FreebaseProperty]
