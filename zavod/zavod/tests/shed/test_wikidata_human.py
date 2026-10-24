from typing import cast

from nomenklatura.wikidata import WikidataClient

from zavod.meta import Dataset
from zavod.shed.wikidata.human import fetch_human
from zavod.shed.wikidata.country import is_abolished
from zavod.tests.shed.test_wikidata_position import StubClient
from zavod.tests.util import make_context


def human_data(qid: str, redirect_to: str | None = None) -> dict:
    data: dict = {
        "id": qid,
        "labels": {"en": {"language": "en", "value": "Jane Doe"}},
        "claims": {
            "P31": [
                {
                    "id": f"{qid}$0",
                    "rank": "normal",
                    "mainsnak": {
                        "snaktype": "value",
                        "property": "P31",
                        "datatype": "wikibase-item",
                        "datavalue": {
                            "type": "wikibase-entityid",
                            "value": {"id": "Q5"},
                        },
                    },
                }
            ]
        },
    }
    if redirect_to is not None:
        data["redirects"] = {"from": qid, "to": redirect_to}
    return data


def test_fetch_human_follows_redirect(testdataset1: Dataset):
    context = make_context(testdataset1)
    client = StubClient({"Q100": human_data("Q100", redirect_to="Q200")})

    fetched = fetch_human(context, cast(WikidataClient, client), "Q100")
    assert fetched is not None
    item, entity = fetched
    assert item.id == "Q200"
    assert entity.id == "Q200"


def test_fetch_human_rejects_missing_and_non_human(testdataset1: Dataset):
    context = make_context(testdataset1)
    data = human_data("Q300")
    data["claims"] = {}
    client = StubClient({"Q300": data})

    assert fetch_human(context, cast(WikidataClient, client), "Q300") is None
    assert fetch_human(context, cast(WikidataClient, client), "Q404") is None


def test_is_abolished():
    assert is_abolished(None) is False
    assert is_abolished("1989-11-09") is True
    assert is_abolished("1991-12-26") is False
