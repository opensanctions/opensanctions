from opensanctions.core.target import Target
from opensanctions.core.dataset import Dataset


class Collection(Target):
    """A grouping of individual data sources. Data sources are bundled in order
    to be more useful than any individual source."""

    TYPE = "collection"

    def __init__(self, file_path, config):
        super().__init__(self.TYPE, file_path, config)

    @property
    def datasets(self):
        targets = set()
        for target in Target.all():
            if self.name in target.collections:
                targets.update(target.datasets)
        return set([t for t in targets if t.TYPE == Dataset.TYPE])
