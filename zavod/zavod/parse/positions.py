from typing import Optional, Iterable
from zavod.context import Context
from zavod.entity import Entity

from followthemoney.util import join_text, make_entity_id
from normality import slugify


def make_position(
    context: Context,
    name: str,
    summary: Optional[str] = None,
    description: Optional[str] = None,
    country: Optional[str | Iterable[str]] = None,
    subnational_area: Optional[str] = None,
    organization: Optional[str] = None,
    inception_date: Optional[Iterable[str]] = None,
    dissolution_date: Optional[Iterable[str]] = None,
    number_of_seats: Optional[str] = None,
    wikidata_id: Optional[str] = None,
    source_url: Optional[str] = None,
    lang: Optional[str] = None,
) -> Entity:
  """Create consistent position entities. Help make sure the same position
  from different sources will end up with the same id, while different positions
  don't end up overriding each other."""

  position = context.make("Position")

  position.add("name", name, lang=lang)
  position.add("summary", summary, lang=lang)
  position.add("description", description, lang=lang)
  position.add("country", country, lang=lang)
  position.add("organization", organization, lang=lang)
  position.add("subnationalArea", subnational_area, lang=lang)
  position.add("inceptionDate", inception_date)
  position.add("dissolutionDate", dissolution_date)
  position.add("numberOfSeats", number_of_seats)
  position.add("wikidataId", wikidata_id)
  position.add("sourceUrl", source_url)

  parts = [
    name,
    country,
    inception_date,
    dissolution_date,
  ]
  hash_id = make_entity_id(*parts)
  if hash_id is not None:
      position.id = f"pos-{hash_id}"

  return position
