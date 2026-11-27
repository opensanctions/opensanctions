"""Decide from a position annotation whether the position is a PEP position.

The rules follow the FATF Guidance on Politically Exposed Persons, June 2013
(https://www.fatf-gafi.org/content/dam/fatf-gafi/guidance/Guidance-PEP-Rec12-22.pdf),
cited by paragraph, and the levels and roles listed in the OpenSanctions PEP
methodology (https://www.opensanctions.org/docs/pep/methodology/).
"""

from enum import StrEnum

from models import (
    LEVEL_FREE_ROLES,
    Annotation,
    AnnotationRecord,
    Level,
    Role,
    Seniority,
)


class PepDecision(StrEnum):
    PEP = "pep"
    NOT_PEP = "not_pep"
    # The annotation leaves open a value that can change the decision.
    INSUFFICIENT = "insufficient"


GOVERNMENT_LEVELS: frozenset[Level] = frozenset(
    {"gov.national", "gov.state", "gov.muni", "gov.igo"}
)

# The levels at which the leadership and senior bands of a role are PEP positions.
# FATF ¶38 leaves the threshold for each level to the jurisdiction, so these sets
# follow the OpenSanctions methodology.
PEP_LEVELS: dict[Role, frozenset[Level]] = {
    "gov.executive": GOVERNMENT_LEVELS,
    "gov.legislative": GOVERNMENT_LEVELS,
    "gov.judicial": frozenset({"gov.national", "gov.state", "gov.igo"}),
    "gov.admin": frozenset({"gov.national", "gov.igo"}),
    "gov.security": frozenset({"gov.national", "gov.igo"}),
    "gov.financial": frozenset({"gov.national", "gov.igo"}),
    # SOE level is often hard to establish, and control at any level suffices.
    "gov.soe": GOVERNMENT_LEVELS,
}


def classify_role(role: Role, level: Level, seniority: Seniority) -> PepDecision:
    """Decide for one role of a valid annotation.

    The tree checks role, then seniority, then level: role decides whether the
    other two matter, and seniority ends most branches before level is needed."""
    if role == "undecided":
        raise ValueError("An undecided role has no decision tree of its own.")

    # Role
    if role == "none":
        return PepDecision.NOT_PEP
    if role == "gov.head":
        # FATF ¶11 and ¶36 name heads of state or government.
        return PepDecision.PEP

    # Seniority: FATF ¶37 excludes middle-ranking and junior officials.
    if seniority == "junior":
        return PepDecision.NOT_PEP
    if seniority == "middle":
        # FATF ¶40 names deputy ministers, a middle rank in the codebook.
        if role != "gov.executive" or level not in ("gov.national", "undecided"):
            return PepDecision.NOT_PEP
        if level == "undecided":
            return PepDecision.INSUFFICIENT
        return PepDecision.PEP

    # Level
    if role not in LEVEL_FREE_ROLES:
        pep_levels = PEP_LEVELS[role]
        if level == "undecided":
            if pep_levels != GOVERNMENT_LEVELS:
                return PepDecision.INSUFFICIENT
        elif level not in pep_levels:
            return PepDecision.NOT_PEP

    # FATF ¶11 names senior officials. An undecided band can still be junior.
    if seniority == "undecided":
        return PepDecision.INSUFFICIENT
    assert seniority in ("leadership", "senior"), seniority
    return PepDecision.PEP


def classify_undecided_role(level: Level, seniority: Seniority) -> PepDecision:
    """Decide for an annotation without a role, where only the band can exclude
    every role. gov.head is always leadership, so it is not a candidate here."""
    if seniority in ("junior", "none"):
        return PepDecision.NOT_PEP
    # Only a national deputy minister is a PEP position at the middle band (FATF ¶40).
    if seniority == "middle" and level not in ("gov.national", "undecided"):
        return PepDecision.NOT_PEP
    return PepDecision.INSUFFICIENT


def classify_pep(annotation: Annotation) -> PepDecision:
    """Decide from the level, roles and seniority of one annotation."""
    if annotation.roles == ["undecided"]:
        return classify_undecided_role(annotation.level, annotation.seniority)
    decisions = {
        classify_role(role, annotation.level, annotation.seniority)
        for role in annotation.roles
    }
    # One PEP role makes the position a PEP position.
    if PepDecision.PEP in decisions:
        return PepDecision.PEP
    if PepDecision.INSUFFICIENT in decisions:
        return PepDecision.INSUFFICIENT
    return PepDecision.NOT_PEP


def decide_pep(record: AnnotationRecord) -> PepDecision:
    """Decide from the human annotation on the latest reviewed primary annotation,
    else from that primary annotation if the reviewer did not veto it.

    Golden annotations are references for evaluation and do not count here."""
    primary = record.latest_primary()
    if primary is None or primary.review is None:
        return PepDecision.INSUFFICIENT
    human = record.human_on(primary)
    if human is not None:
        return classify_pep(human)
    if primary.review.veto:
        return PepDecision.INSUFFICIENT
    return classify_pep(primary)
