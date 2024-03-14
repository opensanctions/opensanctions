#!/bin/bash
#
# Lists the datasets that have changed between the two given refs.

set -euf -o pipefail

>&2 echo "base: $1"
>&2 echo "head: $2"

dir_names=$(git diff --name-only --diff-filter=ACMRT $1 $2  datasets | xargs dirname | grep -v _collections | sort --unique | xargs)
find $dir_names -regextype egrep -regex ".+\.ya?ml" | xargs
