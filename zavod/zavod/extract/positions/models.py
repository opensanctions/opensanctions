from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, Self
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

DATA_DIR = Path(__file__).parent / "data"
DATASETS_DIR = Path(__file__).parent / "datasets"
ITEM_NAMESPACE = UUID("2f4f5a8e-1c1e-4a5e-9a52-8c9f2b6d7e10")

Level = Literal["gov.national", "gov.state", "gov.muni", "gov.igo", "none", "undecided"]
Role = Literal[
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
Seniority = Literal["leadership", "senior", "middle", "junior", "undecided"]

LEVELS: tuple[Level, ...] = Level.__args__  # type: ignore[attr-defined]
ROLES: tuple[Role, ...] = Role.__args__  # type: ignore[attr-defined]
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


class DatasetConfig(BaseModel):
    """Annotation settings for one dataset, stored in DATASETS_DIR as <name>.yml."""

    model_config = ConfigDict(extra="forbid")

    prompt_addendum: str | None = None


class Annotation(BaseModel):
    """The level, role and seniority of a position, as defined in codebook.md."""

    level: Level = Field(
        description=(
            "The governing jurisdiction. 'none' if no government level applies, "
            "'undecided' if one applies but the evidence does not identify it."
        )
    )
    roles: list[Role] = Field(
        description="The roles of the office. Empty if the evidence does not establish a role."
    )
    seniority: Seniority = Field(
        description=(
            "The rank within the level/role combination. 'undecided' if the "
            "evidence does not establish it."
        )
    )

    def labels(self) -> "Annotation":
        return Annotation(
            level=self.level, roles=list(self.roles), seniority=self.seniority
        )


class GoldenAnnotation(Annotation):
    """A reference annotation for evaluation."""

    type: Literal["golden"] = "golden"
    note: str = Field(description="Why the item is in the golden set.")


class Review(BaseModel):
    created_at: datetime
    model: str
    reasoning: str
    veto: bool


class PrimaryAnnotation(Annotation):
    type: Literal["primary"] = "primary"
    id: UUID
    created_at: datetime
    model: str
    # A hash of the item as rendered for the models, including the dataset context.
    context_hash: str
    key_evidence: str
    reasoning: str
    review: Review | None = None


class HumanAnnotation(Annotation):
    """A human decision on a vetoed primary annotation."""

    type: Literal["human"] = "human"
    created_at: datetime
    author: str
    target: UUID
    note: str | None = None


AnyAnnotation = Annotated[
    GoldenAnnotation | PrimaryAnnotation | HumanAnnotation,
    Field(discriminator="type"),
]


class AnnotatorResponse(BaseModel):
    key_evidence: str = Field(
        description=(
            "Facts stated in the label and its context, or found through web "
            "research. For each web fact, give the source URL and quote the "
            "passage that states it verbatim. No inferences."
        )
    )
    reasoning: str = Field(
        description="How the codebook rules apply to the key evidence."
    )
    annotation: Annotation


class VetoResponse(BaseModel):
    reasoning: str = Field(description="Why the annotation is safe or unsafe.")
    veto: bool = Field(description="True to escalate the annotation to a human.")


class AnnotationRecord(BaseModel):
    id: UUID
    item: Item
    annotations: list[AnyAnnotation] = []

    @model_validator(mode="after")
    def check_annotations(self) -> Self:
        if len([a for a in self.annotations if isinstance(a, GoldenAnnotation)]) > 1:
            raise ValueError(f"Record {self.id} has more than one golden annotation.")
        primary_ids = {a.id for a in self.primaries()}
        targets = [a.target for a in self.annotations if isinstance(a, HumanAnnotation)]
        if not set(targets) <= primary_ids:
            raise ValueError(f"Record {self.id} has a human annotation without target.")
        if len(set(targets)) != len(targets):
            raise ValueError(
                f"Record {self.id} has two human annotations on one target."
            )
        return self

    def primaries(self) -> list[PrimaryAnnotation]:
        return [a for a in self.annotations if isinstance(a, PrimaryAnnotation)]

    def latest_primary(self) -> PrimaryAnnotation | None:
        primaries = self.primaries()
        return primaries[-1] if primaries else None

    def golden(self) -> GoldenAnnotation | None:
        for annotation in self.annotations:
            if isinstance(annotation, GoldenAnnotation):
                return annotation
        return None

    def human_on(self, primary: PrimaryAnnotation) -> HumanAnnotation | None:
        for annotation in self.annotations:
            if (
                isinstance(annotation, HumanAnnotation)
                and annotation.target == primary.id
            ):
                return annotation
        return None

    def final_annotation(self) -> Annotation | None:
        """The golden annotation, else the human decision on the latest primary
        annotation, else the latest primary annotation if the reviewer approved it."""
        golden = self.golden()
        if golden is not None:
            return golden.labels()
        primary = self.latest_primary()
        if primary is None:
            return None
        human = self.human_on(primary)
        if human is not None:
            return human.labels()
        if primary.review is not None and not primary.review.veto:
            return primary.labels()
        return None


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
