from collections.abc import Sequence
from pathlib import Path
from typing import Literal, Self
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

DATA_DIR = Path(__file__).parent / "data"
ITEM_NAMESPACE = UUID("2f4f5a8e-1c1e-4a5e-9a52-8c9f2b6d7e10")

Level = Literal["gov.national", "gov.state", "gov.muni", "gov.igo", "none", "undecided"]
Function = Literal[
    "gov.head",
    "gov.executive",
    "gov.legislative",
    "gov.judicial",
    "gov.admin",
    "gov.security",
    "gov.financial",
    "gov.soe",
    "role.diplo",
    "pol.party",
    "gov.religion",
]
Seniority = Literal["leadership", "senior", "other", "undecided"]

LEVELS: tuple[Level, ...] = Level.__args__  # type: ignore[attr-defined]
FUNCTIONS: tuple[Function, ...] = Function.__args__  # type: ignore[attr-defined]
SENIORITIES: tuple[Seniority, ...] = Seniority.__args__  # type: ignore[attr-defined]


class Item(BaseModel):
    """A position label in the context it was published in."""

    model_config = ConfigDict(frozen=True)

    caption: str
    countries: tuple[str, ...]
    subnational_areas: tuple[str, ...]
    dataset: str

    @property
    def id(self) -> UUID:
        return uuid5(ITEM_NAMESPACE, self.model_dump_json())


class Annotation(BaseModel):
    """The level, function and seniority of a position, as defined in codebook.md."""

    out_of_scope: bool = Field(
        description="True if the position is a confirmed private or unrelated role."
    )
    level: Level | None = Field(
        description=(
            "The governing jurisdiction. 'none' if no government level applies, "
            "'undecided' if one applies but the evidence does not identify it. "
            "Null if out of scope."
        )
    )
    functions: list[Function] = Field(
        description=(
            "The functions of the office. Empty if out of scope or if the evidence "
            "does not establish a function."
        )
    )
    seniority: Seniority | None = Field(
        description="The rank the title denotes. Null if out of scope."
    )

    @model_validator(mode="after")
    def check_scope(self) -> Self:
        if self.out_of_scope:
            if self.level is not None or self.functions or self.seniority is not None:
                raise ValueError("An out-of-scope annotation must not assign labels.")
        elif self.level is None or self.seniority is None:
            raise ValueError("An in-scope annotation must assign level and seniority.")
        return self


class AnnotatorResponse(BaseModel):
    reasoning: str = Field(
        description="The evidence from the label and the codebook rules that apply."
    )
    annotation: Annotation


class VetoResponse(BaseModel):
    reasoning: str = Field(description="Why the annotation is safe or unsafe.")
    veto: bool = Field(description="True to escalate the annotation to a human.")


class AnnotationRecord(BaseModel):
    id: UUID
    item: Item
    primary_model: str | None = None
    primary: AnnotatorResponse | None = None
    review_model: str | None = None
    review: VetoResponse | None = None
    human: Annotation | None = None
    # The primary annotation if the reviewer approves it, otherwise the human one.
    annotation: Annotation | None = None


def read_items(path: Path) -> list[Item]:
    with path.open() as fh:
        return [Item.model_validate_json(line) for line in fh if line.strip()]


def write_jsonl(path: Path, models: Sequence[BaseModel]) -> None:
    """Write via a temporary file so an interrupt never leaves a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w") as fh:
        for model in models:
            fh.write(model.model_dump_json() + "\n")
    tmp_path.replace(path)
