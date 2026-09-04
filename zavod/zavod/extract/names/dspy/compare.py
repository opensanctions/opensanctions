import json
from pathlib import Path
from typing import Any

from followthemoney import Model

from zavod.context import Context
from zavod.extract.names.clean import Names, SourceNames, clean_names
from zavod.extract.names.dspy.clean import load_optimised_module
from zavod.extract.names.dspy.example_data import FIELDS, load_data
from zavod.extract.names.dspy.optimise import (
    metric_with_feedback,
    metric_with_feedback_dict,
)
from zavod.meta.dataset import Dataset


def compare_single_entity(examples_path: Path, output_path: Path) -> None:
    program = load_optimised_module()

    _train_set, _val_set, test_set = load_data(examples_path)

    fake_dataset: Dataset = Dataset({"name": "fake"})
    context = Context(fake_dataset)

    results = []

    for example in test_set:
        print("Strings:", example.strings)
        gold = example.toDict()
        del gold["strings"]
        dspy_result = program(
            strings=example.strings, entity_schema=example.entity_schema
        )
        dspy_eval = metric_with_feedback(example, dspy_result)

        schema = Model.instance().get(example.entity_schema)
        assert schema is not None, example.entity_schema
        original = Names(name=example.strings)
        raw_names = SourceNames(entity_schema=schema.name, original=original)

        direct_gpt_result = clean_names(context, raw_names)

        direct_gpt_eval = metric_with_feedback_dict(
            example.toDict(), direct_gpt_result.model_dump()
        )

        agree = True
        for field in FIELDS:
            if set(dspy_result.toDict()[field]) != set(
                direct_gpt_result.model_dump().get(field, [])
            ):
                agree = False
        result = {
            "strings": example.strings,
            "schema": example.entity_schema,
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
    print_scores(results)


def rescore_single_entity(results_path: Path) -> None:
    """Re-score the outputs saved by a previous compare run with the current metric.

    This isolates the effect of a metric change from LLM non-determinism: no new
    LLM calls are made.
    """
    with open(results_path, encoding="utf-8") as results_file:
        results = json.load(results_file)
    for result in results:
        for key in ("dspy_result", "direct_gpt_result"):
            evaluation = metric_with_feedback_dict(
                result["gold"], result[key]["output"]
            )
            result[key]["score"] = evaluation.score
            result[key].pop("feedback", None)
            if evaluation.score < 1.0:
                result[key]["feedback"] = evaluation.feedback
    print_scores(results)


def print_scores(results: list[dict[str, Any]]) -> None:
    if len(results) == 0:
        raise ValueError("No examples in the test split.")
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
