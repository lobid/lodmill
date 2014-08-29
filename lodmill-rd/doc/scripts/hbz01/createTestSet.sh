#!/bin/bash

SOURCE_ROOT="/files/open_data/closed/"
SOURCE_PATH=$SOURCE_ROOT/hbzvk/snapshot
TARGET=/files/open_data/closed/hbzvk/test/
TMP_FILE=testIdsTmp.txt

# hbz
# include all junit test records
echo "building hbz01 test set ..."
tar fjt  ../../../src/test/resources/hbz01XmlClobs.tar.bz2 | cut -d '/' -f6 > $TMP_FILE
# include fix list
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
rm $TARGET_ITEM
hadoop fs -rm $HDFS_FILE

# include all gnd id's of the data which was just build
for i in $(ls /files/open_data/closed/hbzvk/test/); do
	a=$(bzcat  /files/open_data/closed/hbzvk/test/$i | tr '>' '\n' | sed -n 's#^(DE-588)\(.*\)</sub.*#\1#p' )
	for aa in $a; do
		echo "$i has gnd id $aa"
		b=$(echo $aa | sed 's#^\(...\).*#\1#g')
		cat $SOURCE_PATH_ITEM/$b/$aa.nt >> $TARGET_ITEM
	done
done

# done with gnd already - nothing to transform and convert
cat $TARGET_ITEM |  hadoop dfs -put - $HDFS_FILE
