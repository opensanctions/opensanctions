from zavod.parse import make_position
from zavod.context import Context
from zavod.meta import Dataset

def test_make_position(vdataset: Dataset):
  context = Context(vdataset)
  name = "Minister of finance"
  de = make_position(context, name=name, country="de")
  de_with_date = make_position(context, name=name, country="de", inception_date="2021-01-01")
  uk = make_position(context, name=name, country="uk")

  assert de.id != de_with_date.id
  assert de.id != uk.id
  assert de.get("name") == uk.get("name")

def test_make_position(vdataset: Dataset):
  context = Context(vdataset)
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
    lang="en"
  )
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
