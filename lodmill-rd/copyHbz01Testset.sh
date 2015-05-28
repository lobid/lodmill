#!/bin/bash
# Check difference between test data and newly created test data. Convenient when
# updating mapping. If diff is ok, you copy the new test data to the test data.
# Paramter 1: use "nt" or "json"

FILE_TYPE="$1"
sort -g hbz01.$FILE_TYPE |
sort -u > hbz01.$FILE_TYPE.sorted

sort -g src/test/resources/hbz01.$FILE_TYPE |
sort -u >b

diff b hbz01.$FILE_TYPE.sorted
rm b
echo "^<=old vs. ^>new"
echo  "cp hbz01.$FILE_TYPE.sorted src/test/resources/hbz01.$FILE_TYPE"

IFS="
";
