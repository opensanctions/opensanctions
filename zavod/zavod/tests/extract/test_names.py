from zavod.extract.names.dspy.optimise import metric_with_feedback_dict

GOLD = {
    "strings": ["Jonathan (Jonno) Doe"],
    "name": ["Jonathan Doe"],
    "alias": ["Jonno Doe"],
    "weakAlias": [],
    "previousName": [],
}


def test_metric_perfect() -> None:
    prediction = {
        "name": ["Jonathan Doe"],
        "alias": ["Jonno Doe"],
        "weakAlias": [],
        "previousName": [],
    }
    evaluation = metric_with_feedback_dict(GOLD, prediction)
    assert evaluation.score == 1.0
    assert evaluation.feedback == "All names correct."


def test_metric_perfect_ignores_order_and_missing_fields() -> None:
    gold = {"name": ["B", "A"], "alias": [], "weakAlias": [], "previousName": []}
    assert metric_with_feedback_dict(gold, {"name": ["A", "B"]}).score == 1.0


def test_metric_missed_and_extra() -> None:
    prediction = {
        "name": ["Jonathan Doe"],
        "alias": ["Jonno"],
        "weakAlias": [],
        "previousName": [],
    }
    evaluation = metric_with_feedback_dict(GOLD, prediction)
    # 1 exact of 2 gold names, plus 1 extra: 1 / (2 + 1)
    assert abs(evaluation.score - 1 / 3) < 1e-9
    assert "Missed the alias 'Jonno Doe'." in evaluation.feedback
    assert "'Jonno' should not be in alias" in evaluation.feedback
    assert "Jonathan Doe" not in evaluation.feedback


def test_metric_casing_and_punctuation() -> None:
    prediction = {"name": ["jonathan doe"], "alias": ["Jonno-Doe"]}
    evaluation = metric_with_feedback_dict(GOLD, prediction)
    assert abs(evaluation.score - 0.7) < 1e-9
    assert (
        "'jonathan doe' should be written exactly as 'Jonathan Doe'"
        in evaluation.feedback
    )
    assert "'Jonno-Doe' should be written exactly as 'Jonno Doe'" in evaluation.feedback


def test_metric_wrong_field_is_partial_credit_not_double_penalty() -> None:
    prediction = {"name": ["Jonathan Doe", "Jonno Doe"]}
    evaluation = metric_with_feedback_dict(GOLD, prediction)
    # One exact, one miscategorised, no extras: (1 + 0.3) / 2
    assert abs(evaluation.score - 0.65) < 1e-9
    assert "'Jonno Doe' belongs in alias, not name." in evaluation.feedback
    assert "should not be in" not in evaluation.feedback


def test_metric_extra_penalty_is_order_independent() -> None:
    gold_a = {"name": ["A"], "alias": ["B"], "weakAlias": [], "previousName": []}
    gold_b = {"name": ["B"], "alias": ["A"], "weakAlias": [], "previousName": []}
    pred_a = {"name": ["A", "X"], "alias": ["B"]}
    pred_b = {"name": ["B", "X"], "alias": ["A"]}
    score_a = metric_with_feedback_dict(gold_a, pred_a).score
    score_b = metric_with_feedback_dict(gold_b, pred_b).score
    assert score_a == score_b
    assert abs(score_a - 2 / 3) < 1e-9


def test_metric_duplicate_across_fields_is_an_extra() -> None:
    gold = {"name": ["Acme Ltd"], "alias": [], "weakAlias": [], "previousName": []}
    prediction = {"name": ["Acme Ltd"], "alias": ["Acme Ltd"]}
    evaluation = metric_with_feedback_dict(gold, prediction)
    assert abs(evaluation.score - 0.5) < 1e-9
    assert "'Acme Ltd' should not be in alias" in evaluation.feedback


def test_metric_nothing_extracted() -> None:
    evaluation = metric_with_feedback_dict(GOLD, {})
    assert evaluation.score == 0.0
    assert "Missed the name 'Jonathan Doe'." in evaluation.feedback
    assert "Missed the alias 'Jonno Doe'." in evaluation.feedback
