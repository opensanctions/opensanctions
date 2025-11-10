import random
from pathlib import Path
from typing import Any, Dict, List

import yaml
from normality import slugify
from zavod.settings import OPENAI_API_KEY
from zavod.shed.names.dspy.single_entity import FIELDS, load_data
from zavod.shed.names.dspy.split import init_module
from zavod.shed.names.split import LLM_MODEL_VERSION, SINGLE_ENTITY_PROGRAM_PATH

import dspy  # type: ignore  # type: ignore

FIELDS = ["full_name", "alias", "weak_alias", "previous_name"]
EXAMPLES_PATH = Path(__file__).parent / "single_entity_examples.yml"


class SingleEntitySplitSignature(dspy.Signature):  # type: ignore
    """Names categorised and cleaned of non-name characters."""

    string: str = dspy.InputField()
    full_name: List[str] = dspy.OutputField(
        desc="A list of the names of this entity, potentially in various languages and transliterations."
    )
    alias: list[str] = dspy.OutputField(
        desc="A list of alternative names or nicknames for this entity."
    )
    weak_alias: list[str] = dspy.OutputField(
        desc="A list of names with low confidence or a very low degree of uniqueness in the context of legal entity names."
    )
    previous_name: list[str] = dspy.OutputField(
        desc="A list of names this entity was known by in the past."
    )


def load_data(
    examples_path: Path,
) -> tuple[list[dspy.Example], list[dspy.Example], list[dspy.Example]]:
    with open(examples_path, "r", encoding="utf-8") as f:
        cases = yaml.load(f, Loader=yaml.SafeLoader)

    dspy_dataset = []
    for case in cases:
        for field in FIELDS:
            if field not in case:
                case[field] = []
        num_names = sum([len(case[field]) for field in FIELDS])
        if num_names == 0:
            continue
        dspy_dataset.append(dspy.Example(case).with_inputs("string"))

    random.Random(0).shuffle(dspy_dataset)
    train_set = dspy_dataset[: int(len(dspy_dataset) * 0.33)]
    val_set = dspy_dataset[
        int(len(dspy_dataset) * 0.33) : int(len(dspy_dataset) * 0.66)
    ]
    test_set = dspy_dataset[int(len(dspy_dataset) * 0.66) :]

    return train_set, val_set, test_set


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


def optimise(examples_path: Path, program_path: Path, level: str = "heavy") -> None:
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

    optimized_program.save(program_path, save_program=False)

    for predictor in optimized_program.predictors():
        print("Predictor:", predictor)


@cache
def init_module(signature: dspy.Signature) -> dspy.Predict:
    """Initialise a bare DSPy module for name splitting."""
    lm = dspy.LM(f"openai/{LLM_MODEL_VERSION}", api_key=OPENAI_API_KEY)
    dspy.configure(lm=lm)
    dspy.configure_cache(enable_disk_cache=False, enable_memory_cache=True)
    return dspy.Predict(SingleEntitySplitSignature)


@cache
def load_optimised_module(name: str) -> dspy.Predict:
    """Load the optimised name splitting DSPy module."""
    module = init_module(SingleEntitySplitSignature)
    module.load(SINGLE_ENTITY_PROGRAM_PATH)
    return module
