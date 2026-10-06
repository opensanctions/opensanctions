"""The zavod-side port of the funes web-inspection pipeline: a state
store for the candidate catalogue (datasets, subjects, and their seed
URLs) and the attempt records produced as candidates are captured and
inspected. The catalogue is seeded from the funes brief in each
dataset's metadata at the start of its run. Extracted people and
positions are not persisted: the inspection runner emits them as
FollowTheMoney entities through ``context.emit``."""
