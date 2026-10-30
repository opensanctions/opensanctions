from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from models import (
    Annotation,
    AnnotationRecord,
    HumanAnnotation,
    Item,
    Level,
    PrimaryAnnotation,
    Review,
    Role,
    Seniority,
)
from pep import PepDecision, classify_pep, decide_pep

PEP = PepDecision.PEP
NOT_PEP = PepDecision.NOT_PEP
INSUFFICIENT = PepDecision.INSUFFICIENT


def annotation(level: Level, roles: list[Role], seniority: Seniority) -> Annotation:
    return Annotation(level=level, roles=roles, seniority=seniority)


@pytest.mark.parametrize(
    ("level", "roles", "seniority", "expected"),
    [
        # Fully decided
        ("gov.national", ["gov.head"], "leadership", PEP),
        ("gov.national", ["gov.executive"], "senior", PEP),
        ("gov.national", ["gov.executive"], "middle", PEP),
        ("gov.state", ["gov.executive"], "middle", NOT_PEP),
        ("gov.muni", ["gov.legislative"], "senior", PEP),
        ("gov.muni", ["gov.judicial"], "leadership", NOT_PEP),
        ("gov.state", ["gov.judicial"], "senior", PEP),
        ("gov.state", ["gov.admin"], "leadership", NOT_PEP),
        ("gov.igo", ["gov.admin"], "senior", PEP),
        ("gov.national", ["gov.admin"], "middle", NOT_PEP),
        ("gov.muni", ["gov.soe"], "leadership", PEP),
        ("gov.national", ["gov.soe"], "middle", NOT_PEP),
        ("none", ["role.diplo"], "leadership", PEP),
        ("gov.muni", ["pol.party"], "senior", PEP),
        ("gov.national", ["gov.security"], "junior", NOT_PEP),
        ("none", ["none"], "none", NOT_PEP),
        ("gov.national", ["none"], "senior", NOT_PEP),
        # Multiple roles: one PEP role suffices
        ("gov.muni", ["gov.admin", "gov.legislative"], "senior", PEP),
        ("undecided", ["gov.admin", "gov.legislative"], "senior", PEP),
        ("undecided", ["gov.admin", "gov.judicial"], "senior", INSUFFICIENT),
        ("gov.muni", ["gov.admin", "gov.security"], "senior", NOT_PEP),
        # Level undecided
        ("undecided", ["gov.legislative"], "senior", PEP),
        ("undecided", ["gov.executive"], "leadership", PEP),
        ("undecided", ["gov.executive"], "middle", INSUFFICIENT),
        ("undecided", ["gov.soe"], "senior", PEP),
        ("undecided", ["gov.judicial"], "senior", INSUFFICIENT),
        ("undecided", ["gov.financial"], "leadership", INSUFFICIENT),
        ("undecided", ["gov.admin"], "junior", NOT_PEP),
        ("undecided", ["role.diplo"], "leadership", PEP),
        ("undecided", ["gov.head"], "leadership", PEP),
        # Seniority undecided
        ("gov.national", ["gov.head"], "undecided", PEP),
        ("gov.national", ["gov.legislative"], "undecided", INSUFFICIENT),
        ("gov.muni", ["gov.admin"], "undecided", NOT_PEP),
        ("none", ["pol.party"], "undecided", INSUFFICIENT),
        # Roles undecided
        ("gov.national", ["undecided"], "junior", NOT_PEP),
        ("gov.state", ["undecided"], "middle", NOT_PEP),
        ("gov.national", ["undecided"], "middle", INSUFFICIENT),
        ("undecided", ["undecided"], "middle", INSUFFICIENT),
        ("gov.muni", ["undecided"], "senior", INSUFFICIENT),
        ("gov.national", ["undecided"], "none", NOT_PEP),
        # Everything undecided
        ("undecided", ["undecided"], "undecided", INSUFFICIENT),
    ],
)
def test_classify_pep(
    level: Level, roles: list[Role], seniority: Seniority, expected: PepDecision
) -> None:
    assert classify_pep(annotation(level, roles, seniority)) is expected


@pytest.mark.parametrize(
    ("level", "roles", "seniority"),
    [
        ("none", ["gov.admin"], "senior"),
        ("none", ["gov.head"], "leadership"),
        ("none", ["role.diplo", "gov.soe"], "senior"),
        ("gov.national", ["gov.legislative"], "none"),
        ("none", ["pol.party"], "none"),
    ],
)
def test_annotation_rejects_codebook_violations(
    level: Level, roles: list[Role], seniority: Seniority
) -> None:
    with pytest.raises(ValidationError):
        annotation(level, roles, seniority)


def make_record(
    primary_labels: Annotation | None,
    veto: bool | None,
    human_labels: Annotation | None = None,
) -> AnnotationRecord:
    record = AnnotationRecord(
        item=Item(
            id=1, caption="Mayor", countries=("de",), subnational_areas=(), dataset="x"
        )
    )
    if primary_labels is None:
        return record
    now = datetime.now(UTC)
    primary = PrimaryAnnotation(
        **primary_labels.model_dump(),
        id=uuid4(),
        created_at=now,
        model="model",
        context_hash="hash",
        key_evidence="",
        reasoning="",
        review=(
            None
            if veto is None
            else Review(created_at=now, model="model", reasoning="", veto=veto)
        ),
    )
    record.annotations.append(primary)
    if human_labels is not None:
        record.annotations.append(
            HumanAnnotation(
                **human_labels.model_dump(),
                created_at=now,
                author="tester",
                target=primary.id,
            )
        )
    return record


MAYOR = annotation("gov.muni", ["gov.executive"], "leadership")
CLERK = annotation("gov.muni", ["gov.admin"], "junior")


def test_decide_pep_without_primary() -> None:
    assert decide_pep(make_record(None, None)) is INSUFFICIENT


def test_decide_pep_without_review() -> None:
    assert decide_pep(make_record(MAYOR, veto=None)) is INSUFFICIENT


def test_decide_pep_approved() -> None:
    assert decide_pep(make_record(MAYOR, veto=False)) is PEP


def test_decide_pep_vetoed_without_human() -> None:
    assert decide_pep(make_record(MAYOR, veto=True)) is INSUFFICIENT


def test_decide_pep_human_overrides_veto() -> None:
    assert decide_pep(make_record(MAYOR, veto=True, human_labels=CLERK)) is NOT_PEP


def test_decide_pep_human_overrides_approval() -> None:
    assert decide_pep(make_record(CLERK, veto=False, human_labels=MAYOR)) is PEP
