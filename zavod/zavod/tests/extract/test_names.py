import pytest
from zavod.tests.conftest import has_package


@pytest.mark.skipif(not has_package("dspy"), reason="dspy not installed")
def test_metric_with_feedback_partial():
    # Late import to avoid importing dspy if it's not installed
    from zavod.extract.names.dspy.optimise import metric_with_feedback_dict

    example = {
        "string": "Jonathan (Jonno) Doe",
        "full_name": ["Jonathan Doe"],
        "alias": ["Jono Doe"],
        "weak_alias": [],
        "previous_name": [],
    }
    prediction = {
        "full_name": [
            "Jonathan Doe",
        ],
        "alias": ["Jonno"],
        "weak_alias": [],
        "previous_name": [],
    }

    evaluation = metric_with_feedback_dict(example, prediction)
    assert 0 < evaluation.score < 0.5
    assert "correctly extracted the full_name 'Jonathan Doe'" in evaluation.feedback
    assert "missed the alias 'Jono Doe'" in evaluation.feedback
    assert "incorrectly added 'Jonno'" in evaluation.feedback


@pytest.mark.skipif(not has_package("dspy"), reason="dspy not installed")
def test_metric_with_feedback_perfect():
    # Late import to avoid importing dspy if it's not installed
    from zavod.extract.names.dspy.optimise import metric_with_feedback_dict

    example = {
        "string": "Jonathan (Jonno) Doe",
        "full_name": ["Jonathan Doe"],
        "alias": ["Jono Doe"],
        "weak_alias": [],
        "previous_name": [],
    }
    prediction = {
        "full_name": ["Jonathan Doe"],
        "alias": ["Jono Doe"],
        "weak_alias": [],
        "previous_name": [],
    }

    evaluation = metric_with_feedback_dict(example, prediction)
    assert evaluation.score == 1.0
    assert "correctly extracted the full_name 'Jonathan Doe'" in evaluation.feedback
    assert "correctly extracted the alias 'Jono Doe'" in evaluation.feedback
