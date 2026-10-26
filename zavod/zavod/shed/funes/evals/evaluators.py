"""Custom evaluators for the captured-page eval suites.

Classes referenced from dataset YAML must also be listed in
CUSTOM_EVALUATOR_TYPES and passed to ``Dataset.from_file``.
"""

from collections import Counter
from dataclasses import dataclass
from typing import Any

from pydantic_evals.evaluators import EvaluationReason, Evaluator, EvaluatorContext

from zavod.shed.funes.evals.models import FixtureInput
from zavod.shed.funes.extract import Hit, PageResult, Person, Position


def _norm(value: str) -> str:
    return " ".join(value.casefold().split())


def _norm_optional(value: str | None) -> str | None:
    return None if value is None else _norm(value)


def _persons(hit: Hit) -> dict[str, Person]:
    return {_norm(p.name): p for p in hit.persons}


def _positions(person: Person) -> dict[str, Position]:
    return {_norm(pos.name): pos for pos in person.positions}


def _pairs(hit: Hit) -> set[tuple[str, str]]:
    return {
        (_norm(p.name), _norm(pos.name)) for p in hit.persons for pos in p.positions
    }


def _position_fact(
    position: Position,
) -> tuple[str, str | None, str | None, str | None, str | None]:
    """The exact, evaluated fields of one position.

    ``description`` is intentionally informational and excluded.
    """
    return (
        position.name,
        position.organization,
        position.jurisdiction,
        position.start_date,
        position.end_date,
    )


def _person_fact(
    person: Person,
) -> tuple[
    str,
    str | None,
    tuple[str, ...],
    tuple[tuple[str, str | None, str | None, str | None, str | None], ...],
]:
    """The exact, evaluated fields of one person and their holdings.

    Person, country, and position order do not matter. ``bio`` and position
    ``description`` are intentionally informational and excluded.
    """
    positions = tuple(
        sorted((_position_fact(pos) for pos in person.positions), key=repr)
    )
    return person.name, person.dob, tuple(sorted(person.countries)), positions


def _graph(hit: Hit) -> Counter[tuple[Any, ...]]:
    """Order-independent exact person-position graph, preserving duplicates."""
    return Counter(_person_fact(person) for person in hit.persons)


def _f1(expected: set[Any], actual: set[Any]) -> float:
    if not expected:
        return 1.0 if not actual else 0.0
    if not actual:
        return 0.0
    tp = len(expected & actual)
    if tp == 0:
        return 0.0
    precision = tp / len(actual)
    recall = tp / len(expected)
    return 2 * precision * recall / (precision + recall)


def _field_accuracy(
    matched: list[tuple[Any, Any]], field: str, has_expected: bool
) -> float:
    """Share of matched entities whose *field* agrees (normalized, None-safe).

    Vacuous when there is nothing to compare: 1.0 when the expectation itself
    carries no entities, else 0.0 (the expected entities were all missed, which
    the F1 scores already punish).
    """
    if not matched:
        return 0.0 if has_expected else 1.0
    agree = sum(
        _norm_optional(getattr(e, field)) == _norm_optional(getattr(a, field))
        for e, a in matched
    )
    return agree / len(matched)


def _countries_accuracy(
    matched: list[tuple[Person, Person]], has_expected: bool
) -> float:
    if not matched:
        return 0.0 if has_expected else 1.0
    agree = sum(
        {_norm(c) for c in e.countries} == {_norm(c) for c in a.countries}
        for e, a in matched
    )
    return agree / len(matched)


@dataclass
class InspectionF1(Evaluator[FixtureInput, PageResult]):
    """Score a ``Hit`` expectation against the inspection output, structurally
    then per field.

    Attached per-case to hit expectations. Assertion: ``hit_match`` requires an
    order-independent exact match of every structured person and position field
    except ``bio`` and position ``description``. Exact means source spelling,
    capitalization, punctuation, and nulls are significant. Scores: normalized
    person F1, person-position pair F1, and — over persons and positions matched
    by normalized name — per-field accuracy for organization, jurisdiction,
    start_date, end_date, and countries. Non-hit outputs score zero on every
    graph score. Scores are informational diagnostics, not assertions.
    """

    def evaluate(
        self, ctx: EvaluatorContext[FixtureInput, PageResult]
    ) -> dict[str, EvaluationReason | float]:
        expected = ctx.expected_output
        assert isinstance(expected, Hit)
        if not isinstance(ctx.output, Hit):
            return {
                "hit_match": EvaluationReason(
                    False,
                    reason=f"output is {ctx.output.kind!r}, not a hit",
                ),
                "person_f1": 0.0,
                "person_position_f1": 0.0,
                "organization_accuracy": 0.0,
                "jurisdiction_accuracy": 0.0,
                "start_date_accuracy": 0.0,
                "end_date_accuracy": 0.0,
                "countries_accuracy": 0.0,
            }
        expected_graph = _graph(expected)
        actual_graph = _graph(ctx.output)
        missing = list((expected_graph - actual_graph).elements())
        extra = list((actual_graph - expected_graph).elements())
        expected_persons = _persons(expected)
        actual_persons = _persons(ctx.output)
        matched_persons = [
            (expected_persons[name], actual_persons[name])
            for name in expected_persons.keys() & actual_persons.keys()
        ]
        matched_positions = [
            (expected_pos[name], actual_pos[name])
            for expected_person, actual_person in matched_persons
            for expected_pos, actual_pos in [
                (_positions(expected_person), _positions(actual_person))
            ]
            for name in expected_pos.keys() & actual_pos.keys()
        ]
        has_persons = bool(expected.persons)
        has_positions = bool(_pairs(expected))
        return {
            "hit_match": EvaluationReason(
                expected_graph == actual_graph,
                reason=f"missing={missing!r} extra={extra!r}",
            ),
            "person_f1": _f1(set(expected_persons), set(actual_persons)),
            "person_position_f1": _f1(_pairs(expected), _pairs(ctx.output)),
            "organization_accuracy": _field_accuracy(
                matched_positions, "organization", has_positions
            ),
            "jurisdiction_accuracy": _field_accuracy(
                matched_positions, "jurisdiction", has_positions
            ),
            "start_date_accuracy": _field_accuracy(
                matched_positions, "start_date", has_positions
            ),
            "end_date_accuracy": _field_accuracy(
                matched_positions, "end_date", has_positions
            ),
            "countries_accuracy": _countries_accuracy(matched_persons, has_persons),
        }


CUSTOM_EVALUATOR_TYPES = [InspectionF1]
