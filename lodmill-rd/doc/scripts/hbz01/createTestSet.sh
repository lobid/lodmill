#!/bin/bash
SOURCE_PATH=/files/open_data/closed/hbzvk/snapshot
TARGET=/files/open_data/closed/hbzvk/test/
TMP_FILE=testIdsTmp.txt

# include all junit test records
tar fjt  ../../../src/test/resources/hbz01XmlClobs.tar.bz2 | cut -d '/' -f6 > $TMP_FILE
cat testIds.txt >> $TMP_FILE
for i in $(cat $TMP_FILE); do
	SOURCE_SUBPATH=$(echo "$i" | sed  -e 's#^..\([0-9][0-9][0-9][0-9][0-9]\).*#\1#')	
	cp $SOURCE_PATH/$SOURCE_SUBPATH/$i.bz2 $TARGET
done

