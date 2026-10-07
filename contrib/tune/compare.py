import json
from pathlib import Path

from clean import load_optimised_module
from example_data import load_data
from optimise import metric_with_feedback


def compare_single_entity(examples_path: Path, output_path: Path) -> None:
    """Evaluate the optimised program on the held-out third of the examples.

    The program runs through ProductionFormatAdapter, so results reflect the
    exact wire format production uses; there is no longer a separate direct
    arm to compare against.
    """
    program = load_optimised_module()

    _train_set, _val_set, test_set = load_data(examples_path)

    results = []

    for example in test_set:
        print("Strings:", example.strings)
        gold = example.toDict()
        del gold["strings"]
        result = program(strings=example.strings, entity_schema=example.entity_schema)
        evaluation = metric_with_feedback(example, result)

        entry = {
            "strings": example.strings,
            "schema": example.entity_schema,
            "gold": gold,
            "result": {
                "output": result.toDict(),
                "score": evaluation.score,
            },
        }
        if evaluation.score < 1.0:
            entry["result"]["feedback"] = evaluation.feedback
        results.append(entry)

    with open(output_path, "w", encoding="utf-8") as results_file:
        json.dump(results, results_file, indent=2, ensure_ascii=False)
    print(f"Wrote {output_path}")

    total_score = sum(entry["result"]["score"] for entry in results)
    print(
        f"Score: {total_score} out of {len(results)} "
        f"({100 * total_score / len(results)}%)"
    )
