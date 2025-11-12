import random
from pathlib import Path

import yaml

import dspy  # type: ignore

FIELDS = ["full_name", "alias", "weak_alias", "previous_name"]
EXAMPLES_PATH = Path(__file__).parent / "single_entity_examples.yml"


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
