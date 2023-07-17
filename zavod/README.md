# OpenSanctions Development Area

This subdirectory contains the code for a refactoring of OpenSanctions. The goals are as follows:

* Allow crawlers to run fully independently.
* Handle datasets with different sizes, up to very large data volumes.
* Have a well-documented crawling framework that makes it easy to contribute.

Previous file-based attempt:
* https://github.com/opensanctions/opensanctions/commit/3c32af1bcd218f0bf17eec48774c9994fd7a4afb 

### zavod

Zavod is the FollowTheMoney data factory. It contains a variety of useful functions for building small and reproducible data pipelines that generate FtM graphs.