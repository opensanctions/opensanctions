from zavod.parse import make_position
from zavod.context import Context
from zavod.meta import Dataset

def test_make_position(vdataset: Dataset):
  context = Context(vdataset)
  name = "Minister of finance"
  de = make_position(context, name=name, country="de")
  de_with_date = make_position(context, name=name, country="de", inceptionDate="2021-01-01")
  uk = make_position(context, name=name, country="uk")

  assert de.id != de_with_date.id
  assert de.id != uk.id
  assert de.get("name") == uk.get("name")