import os

TABLE = os.environ.get('OPENSANCTIONS_TABLE', 'zz_opensanctions')  # noqa

TYPE_PASSPORT = u'passport'
TYPE_NATIONALID = u'nationalid'
TYPE_OTHER = u'other'

GENDER_MALE = 'male'
GENDER_FEMALE = 'female'
