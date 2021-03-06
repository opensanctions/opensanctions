from opensanctions.core.target import Target
from opensanctions.core.source import Source


class Collection(Target):
    """A grouping of individual data sources. Data sources are bundled in order
    to be more useful than any individual source."""

    TYPE = "collection"

    def __init__(self, file_path, config):
        super().__init__(self.TYPE, file_path, config)

    @property
    def sources(self):
        targets = set()
        for target in Target.all():
            if self.name in target.collections:
                targets.update(target.sources)
        return set([t for t in targets if t.TYPE == Source.TYPE])
