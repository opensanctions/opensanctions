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
        topics=["gov.national", "gov.executive"],
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
    assert set(one_with_everything.get("topics")) == set(
        ["gov.national", "gov.executive"]
    )
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
        end_date="2. Jan 2021",
    )

    assert occupancy is not None
    assert occupancy.id == "osv-45f94082953295c5fe58c51dfca41103375b01bf"
    assert occupancy.get("holder") == ["thabo"]
    assert occupancy.get("post") == ["osv-40a302b7f09ea065880a3c840855681b18ead5a4"]
    assert occupancy.get("startDate") == ["2021-01-01"]
    assert occupancy.get("endDate") == ["2021-01-02"]
    assert occupancy.get("status") == ["ended"]

    assert person.get("country") == ["ls"]
    assert person.get("topics") == ["role.pep"]


def test_occupancy_not_same_start_end_id(testdataset1: Dataset):
    """Test that an occupancy with the same start but no end, and one with the
    same end but no start, don't end up with the same ID. This occurs in the wild
    when a source has an unknown start date, ends a term, then starts the next
    term."""
    context = Context(testdataset1)
    pos = make_position(context, name="A position", country="ls")
    person = context.make("Person")
    person.id = "thabo"

    def make(implies, start, end):
        return make_occupancy(
            context, person, pos, implies, datetime(2021, 1, 1), start, end
        )

    current_no_end = make(True, "2020-01-01", None)
    assert current_no_end is not None
    assert current_no_end.get("status") == ["current"]

    ended_no_start = make(True, None, "2020-01-01")
    assert ended_no_start is not None
    assert ended_no_start.get("status") == ["ended"]

    assert ended_no_start.id != current_no_end.id


def test_occupancy_dataset_coverage():
    # If coverage end is in the future, we trust the future end date
    dataset1 = Dataset(
        {
            "name": "dataset1",
            "title": "Dataset 1",
            "coverage": {"end": "2021-01-04"},
            "prefix": "dataset1",
        },
    )
    context1 = Context(dataset1)
    pos = make_position(context1, name="A position", country="ls")
    person = context1.make("Person")
    person.id = "thabo"

    occupancy1 = make_occupancy(
        context1,
        person=person,
        position=pos,
        current_time=datetime(2021, 1, 3),
        start_date="2021-01-01",
        end_date="2021-01-05",
    )
    assert occupancy1 is not None
    assert occupancy1.get("status") == ["current"]

    # If coverage end date has passed, we don't trust the future end date
    dataset2 = Dataset(
        {
            "name": "dataset2",
            "title": "Dataset 2",
            "coverage": {"end": "2021-01-02"},
            "prefix": "dataset2",
        },
    )
    context2 = Context(dataset2)
    pos2 = make_position(context2, name="A position", country="ls")
    person2 = context2.make("Person")
    person2.id = "thabo"

    # all fields
    occupancy2 = make_occupancy(
        context2,
        person=person2,
        position=pos2,
        current_time=datetime(2021, 1, 3),
        start_date="2021-01-01",
        end_date="2021-01-05",
    )
    assert occupancy2 is not None
    assert occupancy2.get("endDate") == ["2021-01-05"]
