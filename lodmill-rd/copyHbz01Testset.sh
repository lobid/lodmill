#!/bin/bash
# Check difference between test data and newly created test data. Convenient when
# updating mapping. If diff is ok, you copy the new test data to the test data.
# Paramter 1: use "nt" or "es.nt"
echo "^<=old vs. ^>new"

case $1 in
	CT) echo "Nice diff on CT003012479.nt"
	diff src/test/resources/CT003012479.nt tmp/nt/00301/CT003012479.nt | sed -e 's# _:[[:alnum:]]*\(.*\)#\1#g' |  sed -e 's#\(.*\)_:[[:alnum:]]*#\1#g' |sed -e 's#^[[:digit:]].*##g' | sort | uniq -u -f2 ;;
	*)
	FILE_TYPE="$1"
	sort -g hbz01.$FILE_TYPE |
	sort -u > hbz01.$FILE_TYPE.sorted

	sort -g src/test/resources/hbz01.$FILE_TYPE |
	sort -u >b

	diff b hbz01.$FILE_TYPE.sorted | sed -e 's# _:[[:alnum:]]*\(.*\)#\1#g' |  sed -e 's#\(.*\)_:[[:alnum:]]*#\1#g' |sed -e 's#^[[:digit:]].*##g' | sort | uniq -u -f2

	rm b
	echo "^<=old vs. ^>new"
	echo  "cp hbz01.$FILE_TYPE.sorted src/test/resources/hbz01.$FILE_TYPE"
esac
