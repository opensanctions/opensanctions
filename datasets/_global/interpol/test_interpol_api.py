from copy import deepcopy
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import pytest
from requests.exceptions import HTTPError

from zavod import Context, Entity
from zavod.meta import load_dataset_from_path

from datasets._global.interpol import interpol_api


class NoticeContext(Context):
    """Use real entity/name handling while capturing output without I/O."""

    def __init__(self) -> None:
        dataset = load_dataset_from_path(
            Path(__file__).with_name("interpol_red_notices.yml")
        )
        assert dataset is not None
        self.dataset = dataset
        self.log = Mock()
        self.emitted: list[Entity] = []

    def emit(
        self,
        entity: Entity,
        external: bool = False,
        origin: str | None = None,
    ) -> None:
        self.emitted.append(entity)


@pytest.fixture
def context(monkeypatch: pytest.MonkeyPatch) -> NoticeContext:
    monkeypatch.setattr(interpol_api, "SEEN_URLS", set())
    monkeypatch.setattr(interpol_api, "SEEN_IDS", set())
    return NoticeContext()


def notice(first: str | None, last: str | None, number: int = 1) -> dict[str, Any]:
    return {
        "entity_id": f"2099/{number}",
        "_links": {
            "self": {
                "href": f"https://ws-public.interpol.int/notices/v1/red/2099-{number}"
            }
        },
        "forename": first,
        "name": last,
        "date_of_birth": "1980/01/02",
        "arrest_warrants": [
            {
                "issuing_country_id": "GB",
                "charge": "Synthetic test charge",
                "charge_translation": None,
            }
        ],
    }


def crawl(
    context: NoticeContext, monkeypatch: pytest.MonkeyPatch, data: dict[str, Any]
) -> None:
    monkeypatch.setattr(context, "fetch_json", Mock(return_value=deepcopy(data)))
    interpol_api.crawl_notice(context, deepcopy(data))


@pytest.mark.parametrize("first,last", [(None, None), ("", " "), ("-", None)])
def test_nameless_notice_emits_neither_person_nor_sanction(
    context: NoticeContext,
    monkeypatch: pytest.MonkeyPatch,
    first: str | None,
    last: str | None,
) -> None:
    crawl(context, monkeypatch, notice(first, last))
    assert context.emitted == []
    cast(Mock, context.log).info.assert_called_once_with(
        "Skipping notice without a name"
    )
    cast(Mock, context.log).warning.assert_not_called()


@pytest.mark.parametrize(
    "first,last,expected",
    [("Jane", None, "Jane"), (None, "Doe", "Doe"), ("Jane", "Doe", "Jane Doe")],
)
def test_named_notice_after_nameless_notice_is_preserved(
    context: NoticeContext,
    monkeypatch: pytest.MonkeyPatch,
    first: str | None,
    last: str | None,
    expected: str,
) -> None:
    crawl(context, monkeypatch, notice(None, None))
    crawl(context, monkeypatch, notice(first, last, number=2))
    assert len(context.emitted) == 2
    sanction, person = context.emitted
    assert person.schema.name == "Person"
    assert person.get("name") == [expected]
    assert person.get("birthDate") == ["1980-01-02"]
    assert set(person.get("topics")) == {"crime", "wanted"}
    assert sanction.schema.name == "Sanction"
    assert sanction.get("entity") == [person.id]
    assert sanction.get("authorityId") == ["2099/2"]
    assert sanction.get("country") == ["gb"]
    assert sanction.get("reason") == ["Synthetic test charge"]


@pytest.mark.parametrize("query", [False, True])
def test_http_error_without_response_is_preserved(
    context: NoticeContext, monkeypatch: pytest.MonkeyPatch, query: bool
) -> None:
    error = HTTPError("Synthetic failure without a response")
    monkeypatch.setattr(context, "fetch_json", Mock(side_effect=error))
    with pytest.raises(HTTPError) as exc:
        if query:
            interpol_api.crawl_query(context, {})
        else:
            interpol_api.crawl_notice(context, notice("Jane", "Doe"))
    assert exc.value is error
    assert context.emitted == []
