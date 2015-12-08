#!/bin/bash

make

DUMP_BASE=$DATA_PATH/tmp/`date +%Y%m%d`
DUMP_PATH=$DUMP_BASE/opennames
ARCHIVE_PATH=$DATA_PATH/opennames/opennames-`date +%Y%m%d`.tgz

mkdir -p $DUMP_PATH
pep dump $DUMP_PATH

cd $DUMP_BASE
tar cfz $ARCHIVE_PATH opennames

aws s3 cp $ARCHIVE_PATH s3://archive.pudo.org/opennames/opennames-`date +%Y%m%d`.tgz
aws s3 cp $ARCHIVE_PATH s3://archive.pudo.org/opennames/opennames.tgz
aws s3 sync $DUMP_PATH $ARCHIVE_PATH s3://archive.pudo.org/opennames/data/
