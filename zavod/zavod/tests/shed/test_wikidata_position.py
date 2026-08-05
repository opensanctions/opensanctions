from typing import Any, cast

from nomenklatura.wikidata import Item, WikidataClient
from nomenklatura.wikidata.lang import LangText

from zavod import settings
from zavod.context import Context
from zavod.meta import Dataset
from zavod.shed.wikidata.position import wikidata_position, wikidata_occupancy
from zavod.stateful.model import position_table
from zavod.stateful.positions import categorise


class StubClient:
    """Offline WikidataClient stand-in: ancestor/traversal lookups only find
    the fixture items passed in, so items are exactly what their JSON says."""

    reference_time = settings.RUN_TIME

    def __init__(self, items: dict[str, dict] | None = None):
        self._items = items or {}

    def fetch_item(self, qid: str, modified_at: Any = None) -> Item | None:
        data = self._items.get(qid)
        if data is None:
            return None
        return Item(cast(WikidataClient, self), dict(data))

    def get_label(self, qid: str) -> LangText:
        return LangText(qid)


def make_item(client: StubClient, qid: str, label: str, claims: list[dict]) -> Item:
    data = {
        "id": qid,
        "labels": {"en": {"language": "en", "value": label}},
        "claims": {},
    }
    for idx, claim in enumerate(claims):
        data["claims"].setdefault(claim["prop"], []).append(
            {
                "id": f"{qid}${idx}",
                "rank": "normal",
                "mainsnak": {
                    "snaktype": "value",
                    "property": claim["prop"],
                    "datatype": "wikibase-item",
                    "datavalue": {
                        "type": "wikibase-entityid",
                        "value": {"id": claim["qid"]},
                    },
                },
                "qualifiers": claim.get("qualifiers", {}),
            }
        )
    return Item(cast(WikidataClient, client), data)


def interpol_sg_item(client: StubClient) -> Item:
    # Modeled on Q111170012 post-enrichment, with a junk headquarters P17
    # thrown in (the FIFA-president pattern):
    return make_item(
        client,
        "Q111170012",
        "Secretary General of Interpol",
        [
            {"prop": "P31", "qid": "Q4164871"},  # position
            {"prop": "P2389", "qid": "Q8475"},  # Interpol
            {"prop": "P17", "qid": "Q39"},  # Switzerland (junk)
        ],
    )


def test_igo_position_enrolls_for_review(testdataset1: Dataset):
    context = Context(testdataset1)
    categorise.cache_clear()
    client = StubClient()

    item = interpol_sg_item(client)
    position = wikidata_position(context, cast(WikidataClient, client), item)
    # Unreviewed international-body positions don't ship...
    assert position is None
    # ...but they enroll in the review DB as undecided, with the registry
    # country instead of the junk P17:
    [row] = context.db.execute(position_table.select()).fetchall()
    assert row.entity_id == "Q111170012"
    assert row.is_pep is None
    assert row.countries == ["zz"]
    assert "gov.igo" in row.topics
    context.close()


def test_igo_position_accepted_after_review(testdataset1: Dataset):
    context = Context(testdataset1)
    categorise.cache_clear()
    client = StubClient()

    item = interpol_sg_item(client)
    assert wikidata_position(context, cast(WikidataClient, client), item) is None
    context.db.execute(
        position_table.update()
        .where(position_table.c.entity_id == "Q111170012")
        .values(is_pep=True)
    )
    categorise.cache_clear()

    position = wikidata_position(context, cast(WikidataClient, client), item)
    assert position is not None
    assert position.get("country") == ["zz"]
    assert "gov.igo" in position.get("topics")
    # The junk P17 must not surface anywhere:
    assert position.get("subnationalArea") == []
    context.close()


def test_db_is_pep_rescues_countryless_position(testdataset1: Dataset):
    """A reviewed is_pep=True row bypasses the country gate even without a
    registry hit — the manual rescue channel."""
    context = Context(testdataset1)
    categorise.cache_clear()
    client = StubClient()

    item = make_item(
        client,
        "Q999999",
        "Some orphaned office",
        [{"prop": "P31", "qid": "Q4164871"}],
    )
    # Without a DB verdict, the country gate drops it entirely — it never
    # even enrolls for review:
    assert wikidata_position(context, cast(WikidataClient, client), item) is None
    assert context.db.execute(position_table.select()).fetchall() == []

    context.db.execute(
        position_table.insert().values(
            entity_id="Q999999",
            caption="Some orphaned office",
            countries=[],
            topics=[],
            dataset=testdataset1.name,
            created_at=settings.RUN_TIME,
            is_pep=True,
        )
    )
    categorise.cache_clear()
    position = wikidata_position(context, cast(WikidataClient, client), item)
    assert position is not None
    assert position.get("country") == []
    context.close()


def test_historical_claim_does_not_kill_position(testdataset1: Dataset):
    """A stale jurisdiction next to a current one contributes nothing —
    it must not drop the whole position."""
    context = Context(testdataset1)
    categorise.cache_clear()
    client = StubClient(items={"Q159": {"id": "Q159", "claims": {}}})

    item = make_item(
        client,
        "Q888888",
        "Governor of Testov Oblast",
        [
            {"prop": "P31", "qid": "Q132050"},  # governor (role.pep)
            {"prop": "P1001", "qid": "Q15180"},  # Soviet Union (historical)
            {"prop": "P1001", "qid": "Q159"},  # Russia
        ],
    )
    position = wikidata_position(context, cast(WikidataClient, client), item)
    assert position is not None
    assert position.get("country") == ["ru"]
    context.close()


def test_igo_occupancy_infers_no_person_country(testdataset1: Dataset):
    context = Context(testdataset1)
    client = StubClient()

    person = context.make("Person")
    person.id = "test-person"

    claim_data = {
        "id": "x$1",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": "P39",
            "datatype": "wikibase-item",
            "datavalue": {"type": "wikibase-entityid", "value": {"id": "Q111170012"}},
        },
        "qualifiers": {
            "P580": [
                {
                    "snaktype": "value",
                    "property": "P580",
                    "datatype": "time",
                    "datavalue": {
                        "type": "time",
                        "value": {
                            "time": "+2020-01-01T00:00:00Z",
                            "precision": 11,
                            "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                        },
                    },
                }
            ]
        },
    }
    from nomenklatura.wikidata.model import Claim

    claim = Claim(cast(WikidataClient, client), claim_data, "P39")

    igo_position = context.make("Position")
    igo_position.id = "wd-igo-pos"
    igo_position.add("name", "Secretary General of Interpol")
    igo_position.add("country", "zz")
    igo_position.add("topics", "gov.igo")
    occupancy = wikidata_occupancy(context, person, igo_position, claim)
    assert occupancy is not None
    assert person.get("country") == []

    national_position = context.make("Position")
    national_position.id = "wd-national-pos"
    national_position.add("name", "Minister of Tests")
    national_position.add("country", "de")
    national_position.add("topics", "gov.national")
    occupancy = wikidata_occupancy(context, person, national_position, claim)
    assert occupancy is not None
    assert person.get("country") == ["de"]
    context.close()
