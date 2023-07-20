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
    subnationalArea: Optional[str] = None,
    organization: Optional[str] = None,
    inceptionDate: Optional[Iterable[str]] = None,
    dissolutionDate: Optional[Iterable[str]] = None,
    numberOfSeats: Optional[str] = None,
    wikidataId: Optional[str] = None,
    sourceUrl: Optional[str] = None,
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
  position.add("subnationalArea", subnationalArea, lang=lang)
  position.add("inceptionDate", inceptionDate)
  position.add("dissolutionDate", dissolutionDate)
  position.add("numberOfSeats", numberOfSeats)
  position.add("wikidataId", wikidataId)
  position.add("sourceUrl", sourceUrl)

  parts = [
    name,
    country,
    inceptionDate,
    dissolutionDate,
  ]
  hash_id = make_entity_id(*parts)
  if hash_id is not None:
      position.id = f"pos-{hash_id}"

  return position
