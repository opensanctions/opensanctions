from pathlib import Path
from typing import Any

from normality import slugify
from zavod.extract.names.dspy.clean import init_module
from zavod.extract.names.dspy.example_data import FIELDS, load_data
from zavod.settings import OPENAI_API_KEY

import dspy  # type: ignore

LEVELS = ["light", "heavy"]


def metric_with_feedback(
    example: dspy.Example,
    prediction: dspy.Prediction,
    trace: Any = None,
    pred_name: Any = None,
    pred_trace: Any = None,
) -> dspy.Prediction:
    gold = example.toDict()
    extraction = prediction.toDict()
    return metric_with_feedback_dict(gold, extraction)


EXACT_CREDIT = 1.0
"""Credit for a name extracted exactly, in the right field."""
NEAR_CREDIT = 0.7
"""Credit for a name in the right field that differs only in casing, punctuation or spacing."""
MISCATEGORISED_CREDIT = 0.3
"""Credit for a name that was extracted but put in the wrong field."""


def _norm(name: str) -> str:
    """Normalised form used to detect near matches: case, punctuation and spacing folded."""
    return slugify(name) or name.strip().lower()


def metric_with_feedback_dict(
    example: dict[str, list[str]],
    pred: dict[str, list[str]],
) -> dspy.Prediction:
    """
    Score a prediction against the gold example and explain the errors.

    Each gold name earns credit for the best match found in the prediction: full credit in
    the right field, reduced credit if only casing/punctuation differ, a little credit if
    it was put in the wrong field, and none if it is missing. Predicted names that match no
    gold name are extras. The score is total credit divided by the number of gold names plus
    the number of extras, so it is between 0 and 1, does not depend on the order fields are
    processed in, and a wrong field is penalised less than a missed or invented name.

    Feedback lists only the errors, since that is what the optimiser can act on.
    """
    feedback: list[str] = []
    credit = 0.0
    used: set[tuple[str, str]] = set()
    """Predicted (field, name) pairs already matched to a gold name."""

    def unused(field: str) -> list[str]:
        return [n for n in dict.fromkeys(pred.get(field, [])) if (field, n) not in used]

    gold_count = 0
    for field in FIELDS:
        for name in example.get(field, []):
            gold_count += 1
            if name in unused(field):
                credit += EXACT_CREDIT
                used.add((field, name))
                continue
            near = [n for n in unused(field) if _norm(n) == _norm(name)]
            if near:
                credit += NEAR_CREDIT
                used.add((field, near[0]))
                feedback.append(
                    f"The {field} '{near[0]}' should be written exactly as '{name}'."
                )
                continue
            elsewhere = [
                (f, n)
                for f in FIELDS
                if f != field
                for n in unused(f)
                if _norm(n) == _norm(name)
            ]
            if elsewhere:
                other_field, other_name = elsewhere[0]
                credit += MISCATEGORISED_CREDIT
                used.add((other_field, other_name))
                feedback.append(
                    f"'{other_name}' belongs in {field}, not {other_field}."
                )
                continue
            feedback.append(f"Missed the {field} '{name}'.")

    extras = [(f, n) for f in FIELDS for n in unused(f)]
    for field, name in extras:
        feedback.append(
            f"'{name}' should not be in {field}: it is not one of the expected names."
        )

    total = gold_count + len(extras)
    score = credit / total if total else 1.0
    if not feedback:
        feedback.append("All names correct.")
    return dspy.Prediction(score=score, feedback=" ".join(feedback))


def optimise_single_entity(
    examples_path: Path,
    program_path: Path,
    level: str = "heavy",
    threads: int | None = 32,
) -> None:
    train_set, val_set, _test_set = load_data(examples_path)

    optimizer = dspy.GEPA(
        metric=metric_with_feedback,
        auto=level,
        num_threads=threads,
        track_stats=False,
        use_merge=False,
        reflection_lm=dspy.LM(
            model="gpt-5", temperature=1.0, max_tokens=32000, api_key=OPENAI_API_KEY
        ),
        seed=0,
    )
    optimized_program = optimizer.compile(
        init_module(), trainset=train_set, valset=val_set
    )

    optimized_program.save(program_path, save_program=False)

    for predictor in optimized_program.predictors():
        print("Predictor:", predictor)
