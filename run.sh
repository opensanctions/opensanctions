#!/bin/bash

ORIG_DATA_PATH=$DATA_PATH
BUILD_PATH=$DATA_PATH/build/`date +%Y%m%d`/opennames
ARCHIVE_PATH=$ORIG_DATA_PATH/opennames-`date +%Y%m%d`.tgz
DATA_PATH=$BUILD_PATH/sources

mkdir -p $DATA_PATH
make

mkdir -p $ORIG_DATA_PATH/sources
cp -R $DATA_PATH $ORIG_DATA_PATH/sources

pep dump $BUILD_PATH

cd $BUILD_PATH/..
tar cfz $ARCHIVE_PATH opennames

aws s3 cp --acl public-read $ARCHIVE_PATH s3://archive.pudo.org/opennames/opennames-`date +%Y%m%d`.tgz
aws s3 cp --acl public-read $ARCHIVE_PATH s3://archive.pudo.org/opennames/opennames-latest.tgz
aws s3 sync --acl public-read $BUILD_PATH s3://archive.pudo.org/opennames/latest/
