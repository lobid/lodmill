#!/bin/bash
# Check difference between test data and newly created test data. Convenient when
# updating mapping. If diff is ok, you copy the new test data to the test data.
# Paramter 1: use "nt" or "es.nt"
echo "CT003012479.nt:"
diff tmp/nt/00301/CT003012479.nt  src/test/resources/CT003012479.nt | sed -e 's#[<>] _:[[:alnum:]]*\ .*##g' | sed -e 's#^[[:digit:]].*##g' | sort -u
echo "----------------------------------------------\n"
FILE_TYPE="$1"
sort -g hbz01.$FILE_TYPE |
sort -u > hbz01.$FILE_TYPE.sorted

sort -g src/test/resources/hbz01.$FILE_TYPE |
sort -u >b

diff b hbz01.$FILE_TYPE.sorted | sed -e 's#[<>] _:[[:alnum:]]*\ .*##g' | sed -e 's#^[[:digit:]].*##g' | sort -u

rm b
echo "^<=old vs. ^>new"
echo  "cp hbz01.$FILE_TYPE.sorted src/test/resources/hbz01.$FILE_TYPE"

IFS="
";

