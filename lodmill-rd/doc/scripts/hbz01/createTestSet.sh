#!/bin/bash

TARGET=/files/open_data/closed/hbzvk/Test/Test1
TEST_FILE="test.tar.bz2"
THIS="$(pwd)"

echo "building hbz01 test set ..."

# include unit test files
cd $TARGET
tar xfj $THIS/../../../src/test/resources/hbz01XmlClobs.tar.bz2
cd -
# include list
for i in $(cat testIds.txt); do
        if [ ! -f $TARGET/$i ]
                then curl "http://lobid.org/resource?id=$i&format=source" > $TARGET/$i
                echo "huhu $i"
        fi
done

rm $TEST_FILE
tar cfj $TEST_FILE $TARGET
