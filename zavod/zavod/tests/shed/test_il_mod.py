from typing import Any

import pytest
import requests_mock

from zavod import Context
from zavod.shed.il_mod import (
    API_URL,
    Item,
    apply_operative_details,
    drop_fallbacks,
    fetch_content,
    fetch_variants,
    is_placeholder,
    pop_blocks,
    split_ids,
)


def make_item(item_id: str, **props: Any) -> Item:
    return {"id": item_id, "route": {"path": f"/x/{item_id}/"}, "properties": props}


def test_fetch_content_pages(vcontext: Context) -> None:
    pages = [
        {"total": 3, "items": [make_item("a"), make_item("b")]},
        {"total": 3, "items": [make_item("c")]},
    ]
    with requests_mock.Mocker() as m:
        m.get(API_URL, [{"json": page} for page in pages])
        items = fetch_content(vcontext, "operative", "en")
        assert m.request_history[1].qs["skip"] == ["2"]
    assert list(items) == ["a", "b", "c"]


def test_fetch_content_short(vcontext: Context) -> None:
    with requests_mock.Mocker() as m:
        m.get(
            API_URL,
            [
                {"json": {"total": 2, "items": [make_item("a")]}},
                {"json": {"total": 2, "items": []}},
            ],
        )
        with pytest.raises(ValueError, match="Got 1 of 2"):
            fetch_content(vcontext, "operative", "en")


def test_fetch_variants(vcontext: Context) -> None:
    def respond(request: Any, context: Any) -> dict[str, Any]:
        lang = request.headers["Accept-Language"]
        return {"total": 1, "items": [make_item("a", fullName=f"name-{lang}")]}

    with requests_mock.Mocker() as m:
        m.get(API_URL, json=respond)
        variants = fetch_variants(vcontext, "operative")
    names = [item["properties"]["fullName"] for item in variants["a"]]
    assert names == ["name-en", "name-he", "name-ar"]


def test_drop_fallbacks() -> None:
    def variants(en: str, he: str, ar: str) -> list[Item]:
        return [make_item("a", fullName=name) for name in (en, he, ar)]

    assert drop_fallbacks(variants("Ali", "עלי", "علي"), "fullName") == (
        "Ali",
        "עלי",
        "علي",
    )
    # Hebrew given as the English and Arabic variants
    assert drop_fallbacks(variants("עלי", "עלי", "עלי"), "fullName") == (
        None,
        "עלי",
        None,
    )
    # Latin names in all variants are kept as English only
    assert drop_fallbacks(variants("Ali", "Ali", "Ali"), "fullName") == (
        "Ali",
        None,
        None,
    )


@pytest.mark.parametrize(
    "value",
    [
        None,
        "Unknown",
        "NOT AVAILABLE",
        " not  available ",
        "Unknown_Name_User",
        "N/A",
        "אנונימי",
    ],
)
def test_is_placeholder(value: str | None) -> None:
    assert is_placeholder(value)


@pytest.mark.parametrize("value", ["Ali", "123", "Unknown Soldier", "EID"])
def test_is_not_placeholder(value: str) -> None:
    assert not is_placeholder(value)


def test_drop_fallbacks_keeps_placeholders() -> None:
    variants = [make_item("a", fullName="Unknown_Name_User") for _ in range(3)]
    assert drop_fallbacks(variants, "fullName") == ("Unknown_Name_User", None, None)


def test_pop_blocks() -> None:
    props: dict[str, Any] = {
        "dates": [{"elementType": "dateBlock", "properties": {"date": "2020-01-01"}}],
        "empty": None,
    }
    assert pop_blocks(props, "dates", "dateBlock") == [{"date": "2020-01-01"}]
    assert pop_blocks(props, "empty", "dateBlock") == []
    assert props == {}
    props = {"dates": [{"elementType": "addressBlock", "properties": {}}]}
    with pytest.raises(ValueError, match="Unexpected block type"):
        pop_blocks(props, "dates", "dateBlock")


def test_split_ids() -> None:
    assert split_ids(None) == []
    assert split_ids("123; 456") == ["123", "456"]
    assert split_ids("מס' דרכון סורי:\n123") == ["מס' דרכון סורי: 123"]


def test_apply_operative_details(vcontext: Context) -> None:
    entity = vcontext.make("Person")
    entity.id = "person"
    props: dict[str, Any] = {
        "datesOfBirth": [
            {"elementType": "dateBlock", "properties": {"date": "1990-05-01"}},
            {"elementType": "dateBlock", "properties": {"date": "9999-09-09"}},
        ],
        "passportDetails": [
            {
                "elementType": "passportDetailsBlock",
                "properties": {
                    "passportNumber": "Unknown",
                    "nationality": "Jordan",
                    "country": None,
                },
            }
        ],
        "identificationDocuments": [
            {
                "elementType": "identificationDocumentBlock",
                "properties": {
                    "identificationNumber": "123",
                    "country": None,
                    "documentType": None,
                },
            },
            {
                "elementType": "identificationDocumentBlock",
                "properties": {
                    "identificationNumber": "NOT AVAILABLE",
                    "country": None,
                    "documentType": None,
                },
            },
        ],
        "phoneNumbers": ["970599000000"],
        "emailAddresses": ["a@example.com"],
        "other": "kept",
    }
    apply_operative_details(vcontext, entity, props, None)
    assert entity.get("birthDate") == ["1990-05-01"]
    assert entity.get("nationality") == ["jo"]
    assert entity.get("idNumber") == ["123"]
    assert entity.get("email") == ["a@example.com"]
    assert props == {"other": "kept"}


def test_apply_operative_details_partial_date(vcontext: Context) -> None:
    entity = vcontext.make("Person")
    entity.id = "person"
    props: dict[str, Any] = {
        "datesOfBirth": [
            {"elementType": "dateBlock", "properties": {"date": "2010-01-01"}}
        ],
        "passportDetails": None,
        "identificationDocuments": None,
        "phoneNumbers": None,
        "emailAddresses": None,
    }
    comments = ['the source states only the year ("2010"), so the day is not real.']
    apply_operative_details(vcontext, entity, props, comments)
    assert entity.get("birthDate") == ["2010"]
