from zavod.extract.names.dspy.review_examples import classify_edit, review_to_candidate


def test_review_to_candidate_unedited() -> None:
    cand = review_to_candidate(
        dataset="ds",
        key="k",
        reviewer="ann",
        source_value={
            "entity_schema": "Person",
            "original": {"name": ["Jon (Jonno) Doe"]},
        },
        original_extraction={"name": "Jon Doe", "weakAlias": ["Jonno"]},
        extracted_data={"name": ["Jon Doe"], "weakAlias": "Jonno"},
    )
    assert not cand.edited
    assert cand.strings == ["Jon (Jonno) Doe"]
    assert cand.as_example() == {
        "strings": ["Jon (Jonno) Doe"],
        "entity_schema": "Person",
        "name": ["Jon Doe"],
        "weakAlias": ["Jonno"],
    }


def test_review_to_candidate_edited_and_classified() -> None:
    cand = review_to_candidate(
        dataset="ds",
        key="k",
        reviewer="ann",
        source_value={
            "entity_schema": "LegalEntity",
            "original": {"name": ["Hon. Jane Roe, MP"], "alias": ["JSC Roe"]},
        },
        original_extraction={"name": "Hon. Jane Roe, MP", "weakAlias": "JSC Roe"},
        extracted_data={"name": "Jane Roe", "abbreviation": "JSC Roe"},
    )
    assert cand.edited
    assert cand.strings == ["Hon. Jane Roe, MP", "JSC Roe"]
    assert cand.expected == {"name": ["Jane Roe"], "abbreviation": ["JSC Roe"]}
    assert cand.edit_kinds == [
        "recategorised weakAlias -> abbreviation",
        "split, trimmed or merged",
    ]


def test_classify_edit_added_and_dropped() -> None:
    kinds = classify_edit({"name": ["Acme"]}, {"name": ["Zeta"]})
    assert kinds == ["added by reviewer", "dropped by reviewer"]
    assert classify_edit({"name": ["acme ltd"]}, {"name": ["Acme Ltd"]}) == [
        "casing or spacing"
    ]
