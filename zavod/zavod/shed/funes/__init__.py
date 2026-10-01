"""This package contains the zavod-side port of the funes web-inspection
pipeline: a state store for the candidate catalogue (datasets, subjects,
and their seed URLs) and the attempt/snapshot-assessment/inspection
records produced as candidates are captured and inspected. Seeding the
catalogue from zavod dataset metadata is planned but not implemented
here. Extracted people and positions are not persisted: they are emitted
as FollowTheMoney entities through ``context.emit`` by the inspection
runner."""
