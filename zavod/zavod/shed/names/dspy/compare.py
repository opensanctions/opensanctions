import json
from pathlib import Path

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.shed.names.dspy.example_data import FIELDS, load_data
from zavod.shed.names.dspy.optimise import (
    metric_with_feedback,
    metric_with_feedback_dict,
)
from zavod.shed.names.dspy.split import load_optimised_module
from zavod.shed.names.split import split_names


def compare_single_entity(examples_path: Path, output_path: Path) -> None:
    program = load_optimised_module()

    _train_set, _val_set, test_set = load_data(examples_path)

    fake_dataset: Dataset = Dataset({"name": "fake"})
    context = Context(fake_dataset)

    results = []

    for example in test_set:
        print("String:", example.string)
        gold = example.toDict()
        del gold["string"]
        dspy_result = program(string=example.string)
        dspy_eval = metric_with_feedback(example, dspy_result)

        direct_gpt_result = split_names(context, example.string)
        direct_gpt_eval = metric_with_feedback_dict(
            example.toDict(), direct_gpt_result.model_dump()
        )

        agree = True
        for field in FIELDS:
            if set(dspy_result.toDict()[field]) != set(
                direct_gpt_result.model_dump()[field]
            ):
                agree = False
        result = {
            "string": example.string,
            "gold": gold,
            "dspy_result": {
                "output": dspy_result.toDict(),
                "score": dspy_eval.score,
            },
            "direct_gpt_result": {
                "output": direct_gpt_result.model_dump(),
                "score": direct_gpt_eval.score,
            },
            "results_agree": agree,
        }
        if direct_gpt_eval.score < 1.0:
            result["direct_gpt_result"]["feedback"] = direct_gpt_eval.feedback
        if dspy_eval.score < 1.0:
            result["dspy_result"]["feedback"] = dspy_eval.feedback

        results.append(result)

    with open(output_path, "w", encoding="utf-8") as results_file:
        json.dump(results, results_file, indent=2, ensure_ascii=False)
    print(f"Wrote {output_path}")

    total_dspy_score = sum(r["dspy_result"]["score"] for r in results)
    total_direct_gpt_score = sum(r["direct_gpt_result"]["score"] for r in results)
    total_agreed = sum(1.0 for r in results if r["results_agree"])
    print(
        f"DSPy score: {total_dspy_score} out of {len(results)} "
        f"({100 * total_dspy_score / len(results)}%)"
    )
    print(
        f"Direct GPT score: {total_direct_gpt_score} out of {len(results)} "
        f"({100 * total_direct_gpt_score / len(results)}%)"
    )
    print(
        f"Agreement: {total_agreed} out of {len(results)} "
        f"({100 * total_agreed / len(results)}%)"
    )
