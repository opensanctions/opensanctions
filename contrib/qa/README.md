# data integrity tests

This directory contains a set of smoke tests to be run after each ETL run to
verify the correct functioning of the pipeline. It should be run before publishing
the resulting data.

Some of the assertions relate to the resolver file maintained by OpenSanctions.org
and will fail against builds of the data created without that file.