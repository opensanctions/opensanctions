from pprint import pprint  # noqa

from pepparser.util import clean_obj


class Emitter(object):

    def __init__(self):
        pass

    def individual(self, data):
        self.entity(data)

    def entity(self, data):
        data = clean_obj(data)
        pprint(data)
