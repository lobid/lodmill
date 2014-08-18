#!/bin/bash

# - filter to only get Englisch and German skos:prefLabel
# - Rewrite URI
# - remove old file from hdfs and write new one


HDFS_FILE="extlod/dewey_preprocessed.nt"
hadoop fs -rm $HDFS_FILE
grep 'skos/core#prefLabel' /files/open_data/closed/dewey/dewey.nt  | egrep "(@de|@en)" | sed -e 's#20../../about\...##g' | sort -u | hadoop dfs -put - $HDFS_FILE
