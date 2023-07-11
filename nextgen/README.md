# OpenSanctions NG

This subdirectory contains the code for a refactoring of OpenSanctions. The goals are as follows:

* Allow crawlers to run fully independently.
* Handle datasets with different sizes, up to very large data volumes.
* Have a well-documented crawling framework




### zavod

Zavod is the FollowTheMoney data factory. It contains a variety of useful functions for building small and reproducible data pipelines that generate FtM graphs.