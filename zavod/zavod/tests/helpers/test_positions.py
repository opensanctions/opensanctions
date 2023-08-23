from datetime import datetime

from zavod.context import Context
from zavod.meta import Dataset
from zavod.helpers.positions import make_position, make_occupancy


def test_make_position(testdataset1: Dataset):
    context = Context(testdataset1)
    name = "Minister of finance"
    de = make_position(context, name=name, country="de")
    de_with_date = make_position(
        context, name=name, country="de", inception_date="2021-01-01"
    )
    uk = make_position(context, name=name, country="uk")

    assert de.id != de_with_date.id
    assert de.id != uk.id
    assert de.get("name") == uk.get("name")


def test_make_position_full(testdataset1: Dataset):
    context = Context(testdataset1)
    org = context.make("Organization")
    org.id = "myorg"
    one_with_everything = make_position(
        context,
        name="boss",
        country="de",
        description="desc",
        summary="sum",
        subnational_area="subnat",
        organization=org,
        inception_date="2021-01-01",
        dissolution_date="2021-01-02",
        number_of_seats="5",
        wikidata_id="Q123",
        source_url="http://example.com",
        lang="en",
    )
    assert one_with_everything.id == "Q123"
    assert one_with_everything.get("name") == ["boss"]
    assert one_with_everything.get("country") == ["de"]
    assert one_with_everything.get("description") == ["desc"]
    assert one_with_everything.get("summary") == ["sum"]
    assert one_with_everything.get("subnationalArea") == ["subnat"]
    assert one_with_everything.get("organization") == ["myorg"]
    assert one_with_everything.get("inceptionDate") == ["2021-01-01"]
    assert one_with_everything.get("dissolutionDate") == ["2021-01-02"]
    assert one_with_everything.get("numberOfSeats") == ["5"]
    assert one_with_everything.get("wikidataId") == ["Q123"]
    assert one_with_everything.get("sourceUrl") == ["http://example.com/"]


def test_make_occupancy(testdataset1: Dataset):
    context = Context(testdataset1)
    pos = make_position(context, name="A position", country="ls")
    person = context.make("Person")
    person.id = "thabo"

    # all fields
    occupancy = make_occupancy(
        context,
        person=person,
        position=pos,
        no_end_implies_current=True,
        current_time=datetime(2021, 1, 3),
        start_date="2021-01-01",
        end_date="2021-01-02",
    )

    assert occupancy.id == "osv-de34e6b03014409e1d8b13aec5264a5f480b5b1d"
    assert occupancy.get("holder") == ["thabo"]
    assert occupancy.get("post") == ["osv-40a302b7f09ea065880a3c840855681b18ead5a4"]
    assert occupancy.get("startDate") == ["2021-01-01"]
    assert occupancy.get("endDate") == ["2021-01-02"]
    assert occupancy.get("status") == ["ended"]

    def make(implies, start, end):
        return make_occupancy(
        context, person, pos, implies, datetime(2021, 1, 1), start, end
    )

    current_no_end = make(True, "1950-01-01", None)
    assert current_no_end.get("status") == ["current"]

    current_with_end = make(True, "1950-01-01", "2021-01-02")
    assert current_with_end.get("status") == ["current"]

    ended = make(True, "1950-01-01", "2020-12-31")
    assert ended.get("status") == ["ended"]

    unknown = make(False, "1950-01-01", None)
    assert unknown.get("status") == ["unknown"]

    none = make(False, "1950-01-01", "2015-01-01")
    assert none is None
