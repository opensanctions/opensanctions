from dataclasses import dataclass
from typing import Any

from pydantic_evals.evaluators import EvaluationReason

from contrib.prompt_evals.evaluators import (
    ItemListMatch,
    describe_edits,
    match_key,
    norm_value,
    pair_items,
)


def test_match_key_strips_acronyms_and_nicknames() -> None:
    assert match_key("Federal Security Service (FSB)") == match_key(
        "Federal Security Service"
    )
    assert match_key('Nicolas "Nicolasito" Maduro') == match_key("Nicolas Maduro")
    assert match_key("Nicolas “Nicolasito” Maduro") == match_key("Nicolas Maduro")
    assert match_key("  Rosneft  ") == "rosneft"
    assert match_key("The Taliban") == match_key("Taliban")


def test_norm_value_treats_null_and_empty_list_alike() -> None:
    assert norm_value(None) == norm_value([])
    assert norm_value(["Russia", "iran"]) == norm_value(["Iran", "russia"])
    assert norm_value("Company") == norm_value("company")
    assert norm_value("Company") != norm_value("Organization")


def test_pair_items_by_name_with_subset_fallback() -> None:
    expected = [{"name": "Mikhail Tsarev"}, {"name": "Gone Person"}, {"name": "Lukoil"}]
    output = [
        {"name": "Mikhail Mikhailovich Tsarev"},
        {"name": "Lukoil"},
        {"name": "Extra Company LLC"},
    ]
    pairs, missing, spurious = pair_items(expected, output, "name")
    assert [(e["name"], o["name"]) for e, o in pairs] == [
        ("Mikhail Tsarev", "Mikhail Mikhailovich Tsarev"),
        ("Lukoil", "Lukoil"),
    ]
    assert missing == ["Gone Person"]
    assert spurious == ["Extra Company LLC"]


def test_pair_items_does_not_pair_on_a_single_shared_token() -> None:
    pairs, missing, spurious = pair_items(
        [{"name": "Ahmed Alif Rauf"}], [{"name": "Ibrahim Aleef Rauf"}], "name"
    )
    assert pairs == []
    assert missing == ["Ahmed Alif Rauf"]
    assert spurious == ["Ibrahim Aleef Rauf"]


@dataclass
class Ctx:
    expected_output: dict[str, Any] | None
    output: dict[str, Any]


def evaluate(expected: dict[str, Any] | None, output: dict[str, Any]) -> dict[str, Any]:
    result = ItemListMatch().evaluate(Ctx(expected, output))  # type: ignore[arg-type]
    assert isinstance(result, dict)
    return {
        k: v.value if isinstance(v, EvaluationReason) else v for k, v in result.items()
    }


def test_item_list_match_perfect() -> None:
    data = {"designees": [{"name": "A", "entity_schema": "Person", "country": None}]}
    same = {"designees": [{"name": "A", "entity_schema": "Person", "country": []}]}
    result = evaluate(data, same)
    assert result["recall"] == 1.0
    assert result["precision"] == 1.0
    assert result["name_exact"] == 1.0
    assert result["country"] == 1.0
    assert result["all_match"] is True


def test_item_list_match_ignores_deprecated_and_reports_unscored_fields() -> None:
    expected = {
        "designees": [{"name": "A (ABC)", "aliases": ["ABC"], "country": ["Iran"]}]
    }
    output = {"designees": [{"name": "A", "country": ["Iran"], "imo": []}]}
    result = evaluate(expected, output)
    assert result["recall"] == 1.0
    assert result["name_exact"] == 0.0
    assert "aliases" not in result
    assert result["unscored_fields"] == "imo"
    assert result["all_match"] is False


def test_item_list_match_reports_missing_names() -> None:
    expected = {"designees": [{"name": "A"}, {"name": "B"}]}
    output = {"designees": [{"name": "A"}]}
    raw = ItemListMatch().evaluate(Ctx(expected, output))  # type: ignore[arg-type]
    assert isinstance(raw, dict)
    recall = raw["recall"]
    assert isinstance(recall, EvaluationReason)
    assert recall.value == 0.5
    assert recall.reason == "missing: B"
    assert raw["all_match"] is False


def test_item_list_match_without_expected_output_does_not_apply() -> None:
    assert ItemListMatch().evaluate(Ctx(None, {"designees": []})) == {}  # type: ignore[arg-type]


def test_describe_edits_lists_added_removed_renamed_and_field_changes() -> None:
    expected: list[dict[str, Any]] = [
        {
            "name": "Nicolas Maduro Guerra",
            "entity_schema": "Person",
            "country": ["Venezuela"],
        },
        {"name": "New Co", "entity_schema": "Company"},
    ]
    original: list[dict[str, Any]] = [
        {
            "name": "Nicolas Nicolasito Maduro Guerra",
            "entity_schema": "Person",
            "country": [],
        },
        {"name": "Turkish Company", "entity_schema": "Company"},
    ]
    edits = describe_edits(expected, original)
    assert edits == [
        "added: New Co",
        "removed: Turkish Company",
        "renamed: 'Nicolas Nicolasito Maduro Guerra' -> 'Nicolas Maduro Guerra'",
        "Nicolas Maduro Guerra country: [] -> ['Venezuela']",
    ]


def test_case_line_numbers(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from contrib.prompt_evals.render import case_line_numbers

    f = tmp_path / "cases.yaml"
    f.write_text("name: ds\ncases:\n- name: sm719\n  inputs: {}\n- name: 'jy2127'\n")
    assert case_line_numbers(f) == {"sm719": 3, "jy2127": 5}


def test_highlight_html_marks_terms_in_text_and_links() -> None:
    from contrib.prompt_evals.render import extracted_strings, highlight_html

    extraction = {
        "designees": [
            {
                "name": "Global Trading Group NV",
                "country": ["Belgium"],
                "related_url": ["https://x.test/p"],
            },
        ]
    }
    terms = extracted_strings(extraction)
    assert terms[0] == "Global Trading Group NV"
    assert "Company" not in terms
    source = (
        "<div><p>OFAC designated <b>GLOBAL Trading Group NV</b>, a Belgium-based firm, "
        'see <a href="https://x.test/p">profile</a>.</p><script>var belgium=1</script></div>'
    )
    out = highlight_html(source, terms)
    assert "<b><mark>GLOBAL Trading Group NV</mark></b>" in out
    assert "a <mark>Belgium</mark>-based" in out
    assert '<a href="https://x.test/p" style="background: #ff0">' in out
    assert "<script>var belgium=1</script>" in out


def test_highlight_text_escapes_and_marks() -> None:
    from contrib.prompt_evals.render import highlight_text

    assert highlight_text("A & b co", ["B CO"]) == "A &amp; <mark>b co</mark>"
