#!/bin/bash

SOURCE_ROOT="/files/open_data/closed/"
SOURCE_PATH=$SOURCE_ROOT/hbzvk/snapshot
TARGET=/files/open_data/closed/hbzvk/test/
TMP_FILE=testIdsTmp.txt

# include all junit test records
echo "building hbz01 test set ..."
tar fjt  ../../../src/test/resources/hbz01XmlClobs.tar.bz2 | cut -d '/' -f6 > $TMP_FILE
cat testIds.txt >> $TMP_FILE
for i in $(cat $TMP_FILE); do
	SOURCE_SUBPATH=$(echo "$i" | sed  -e 's#^..\([0-9][0-9][0-9][0-9][0-9]\).*#\1#')	
	cp $SOURCE_PATH/$SOURCE_SUBPATH/$i.bz2 $TARGET
done

#gnd 
echo "building gnd test set ..."
ITEM_FILE_NT="gndTest.nt"
SOURCE_PATH_ITEM="$SOURCE_ROOT/gnd/gnd_snapshot"
HDFS_FILE="extlod/gnd-test/$ITEM_FILE_NT"
TARGET_ITEM="/files/open_data/closed/gnd/gnd_snapshot/test/$ITEM_FILE_NT"

hadoop fs -rm $HDFS_FILE

for i in $(sed -n 's#.*d-nb\.info/gnd/\(.*\)>.*#\1#p'  ../../../src/test/resources/hbz01.nt);do
	a=$(echo $i | sed 's#\(...\).*#\1#g')
	cat $SOURCE_PATH_ITEM/$a/$i.nt >> $TARGET_ITEM 
done

cat $TARGET_ITEM |  hadoop dfs -put - $HDFS_FILE
