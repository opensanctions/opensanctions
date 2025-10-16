from pathlib import Path
from typing import Any, Dict, List

from normality import slugify
from zavod.settings import OPENAI_API_KEY
from zavod.shed.names.dspy.example_data import FIELDS, load_data
from zavod.shed.names.dspy.split import init_module
from zavod.shed.names.split import SINGLE_ENTITY_PROGRAM_PATH

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


def metric_with_feedback_dict(
    example: Dict[str, List[str]],
    pred: Dict[str, List[str]],
) -> dspy.Prediction:
    feedback = ""
    score = 0.0
    for field in FIELDS:
        gold_set = set([n for n in example[field]])
        gold_set_lower = set([n.lower() for n in example[field]])
        pred_set = set([n for n in pred[field]])
        pred_set_lower = set([n.lower() for n in pred[field]])
        pred_set_slugs = set([slugify(n) for n in pred[field]])
        extra = pred_set - gold_set
        for name in example[field]:
            if name in pred_set:
                feedback += f"You correctly extracted the {field} '{name}'. "
                score += 1
            elif name.lower() in pred_set_lower:
                feedback += f"You extracted the {field} '{name}' correctly, but with incorrect casing. "
                score += 0.7
            elif slugify(name) in pred_set_slugs:
                feedback += f"You extracted the {field} '{name}' correctly, but with minor differences - perhaps in punctuation or spacing. "
                score += 0.7
            else:
                feedback += f"You missed the {field} '{name}'. "
        for name in extra:
            if name.lower() not in gold_set_lower:
                feedback += f"You incorrectly added '{name}' to the {field} field. "
                score = score * 0.8
    score = score / sum([len(example[f]) for f in FIELDS])
    return dspy.Prediction(score=score, feedback=feedback)


def optimise_single_entity(examples_path: Path, level: str = "heavy") -> None:
    train_set, val_set, _test_set = load_data(examples_path)

    optimizer = dspy.GEPA(
        metric=metric_with_feedback,
        auto=level,
        num_threads=32,
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

    optimized_program.save(SINGLE_ENTITY_PROGRAM_PATH, save_program=False)

    for predictor in optimized_program.predictors():
        print("Predictor:", predictor)
