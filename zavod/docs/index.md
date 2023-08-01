# zavod Data Factory

This page contains documentation for the data processing framework used by [OpenSanctions](https://www.opensanctions.org/). It's called `zavod` ([name](https://en.wikipedia.org/wiki/Zavod)) in order to distinguish it from some of the other software components used by the OpenSanctions project.

`zavod` provides a runtime context and a set of helpers for running crawler scripts that capture data from any online source, convert it to the [`followthemoney`](https://followthemoney.tech) data model, store the output and eventually produce the export files used by OpenSanctions' data consumers.

## Getting started

* [Installation](install.md) of zavod on your machine
* [Tutorial](tutorial.md): how to add a crawler

## Further references

* [Data inclusion criteria](https://www.opensanctions.org/docs/criteria/) - what data will be included in OpenSanctions?
* [What is an entity?](https://www.opensanctions.org/docs/entities/) - intro to the notion of data entities.
* [FollowTheMoney](https://followthemoney.tech) data model documentation, and the OpenSanctions [data dictionary](https://www.opensanctions.org/reference/) (we use a subset of the schemata offered by FollowTheMoney).
* [Statement-based data model](https://www.opensanctions.org/docs/statements/) - the data model used by `zavod` during processing.