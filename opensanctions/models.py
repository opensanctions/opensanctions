import logging
import countrynames
from pprint import pprint  # noqa

import attr
from normality import stringify, collapse_spaces, slugify


log = logging.getLogger(__name__)


class Base(object):
    def to_dict(self):
        return attr.asdict(self)


@attr.s
class NameMixIn(Base):
    _name = attr.ib(default=None)
    title = attr.ib(default=None)
    first_name = attr.ib(default=None)
    second_name = attr.ib(default=None)
    third_name = attr.ib(default=None)
    father_name = attr.ib(default=None)
    last_name = attr.ib(default=None)

    @property
    def name(self):
        if self._name is not None:
            return self._name
        names = (self.first_name, self.second_name, self.third_name,
                 self.father_name, self.last_name)
        names = [n for n in names if n is not None]
        if len(names):
            names = ' '.join(names)
            return collapse_spaces(names)

    @name.setter
    def name(self, name):
        name = stringify(name)
        if name is not None:
            name = collapse_spaces(name)
        self._name = name

    def to_dict(self):
        data = super(NameMixIn, self).to_dict()
        data["name"] = self.name
        data.pop("_name")
        return data


@attr.s
class CountryMixIn(Base):
    country_name = attr.ib(default=None)
    country_code = attr.ib(default=None)

    @property
    def country(self):
        return self.country_code or self.country_name

    @country.setter
    def country(self, name):
        self.country_name = name
        self.country_code = countrynames.to_code(name)


@attr.s
class QualityMixIn(Base):
    QUALITY_WEAK = 'weak'
    QUALITY_STRONG = 'strong'

    quality = attr.ib(default=None)


@attr.s
class Alias(NameMixIn, QualityMixIn):
    """An alternate name for an indivdual."""
    entity_id = attr.ib(default=None)
    type = attr.ib(default=None)
    description = attr.ib(default=None)


@attr.s
class Address(CountryMixIn):
    """An address associated with an entity."""
    entity_id = attr.ib(default=None)
    text = attr.ib(default=None)
    note = attr.ib(default=None)
    street = attr.ib(default=None)
    street_2 = attr.ib(default=None)
    postal_code = attr.ib(default=None)
    city = attr.ib(default=None)
    region = attr.ib(default=None)


@attr.s
class Identifier(CountryMixIn):
    """A document issued to an entity."""
    TYPE_PASSPORT = u'passport'
    TYPE_NATIONALID = u'nationalid'
    TYPE_OTHER = u'other'

    entity_id = attr.ib(default=None)
    type = attr.ib(default=None)
    description = attr.ib(default=None)
    number = attr.ib(default=None)
    issued_at = attr.ib(default=None)


@attr.s
class Nationality(CountryMixIn):
    """A nationality associated with an entity."""
    entity_id = attr.ib(default=None)


@attr.s
class BirthDate(QualityMixIn):
    """Details regarding the birth of an entity."""
    entity_id = attr.ib(default=None)
    date = attr.ib(default=None)


@attr.s
class BirthPlace(QualityMixIn, CountryMixIn):
    """Details regarding the birth of an entity."""
    entity_id = attr.ib(default=None)
    place = attr.ib(default=None)
    description = attr.ib(default=None)


@attr.s
class Entity(NameMixIn):
    """A company or person that is subject to a sanction."""

    TYPE_ENTITY = 'entity'
    TYPE_INDIVIDUAL = 'individual'
    TYPE_VESSEL = 'vessel'

    GENDER_MALE = 'male'
    GENDER_FEMALE = 'female'

    id = attr.ib(default=None)
    source = attr.ib(default=None)
    type = attr.ib(default=None)
    summary = attr.ib(default=None)
    function = attr.ib(default=None)
    program = attr.ib(default=None)
    url = attr.ib(default=None)
    gender = attr.ib(default=None)
    listed_at = attr.ib(default=None)
    updated_at = attr.ib(default=None)

    aliases = attr.ib(default=attr.Factory(list))
    addresses = attr.ib(default=attr.Factory(list))
    identifiers = attr.ib(default=attr.Factory(list))
    nationalities = attr.ib(default=attr.Factory(list))
    birth_dates = attr.ib(default=attr.Factory(list))
    birth_places = attr.ib(default=attr.Factory(list))

    @classmethod
    def create(cls, name, *keys):
        keys = [slugify(k, sep='-') for k in keys]
        entity_id = '-'.join([k for k in keys if k is not None])
        entity_id = '%s.%s' % (name, entity_id)
        entity = Entity(source=name, id=entity_id)
        return entity

    def create_alias(self, name=None):
        alias = Alias(entity_id=self.id, name=name)
        self.aliases.append(alias)
        return alias

    def create_address(self):
        address = Address(entity_id=self.id)
        self.addresses.append(address)
        return address

    def create_identifier(self):
        identifier = Identifier(entity_id=self.id)
        self.identifiers.append(identifier)
        return identifier

    def create_nationality(self):
        nationality = Nationality(entity_id=self.id)
        self.nationalities.append(nationality)
        return nationality

    def create_birth_date(self):
        birth_date = BirthDate(entity_id=self.id)
        self.birth_dates.append(birth_date)
        return birth_date

    def create_birth_place(self):
        birth_place = BirthPlace(entity_id=self.id)
        self.birth_places.append(birth_place)
        return birth_place

    def to_dict(self):
        data = super(Entity, self).to_dict()
        # special treatment for names
        data["aliases"] = [alias.to_dict() for alias in self.aliases]
        return data
