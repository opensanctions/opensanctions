#!/bin/bash

OPENNAMES_SOURCE_DATA=$DATA_PATH/`date +%Y%m%d`
mkdir -p $OPENNAMES_SOURCE_DATA
make
