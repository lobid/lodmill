#!/bin/bash
# run as user "hduser"
# Description:
# - get the vocabs turtle from github
# - transform that to ntriples
# - put that into hdfs
 
function getFileAndCopy2Hdfs() {
        rapper -i turtle https://raw.github.com/lobid/vocabs/master/$1.ttl -o ntriples  | hadoop dfs -put - hbzlod/lobid-vocab/$1.nt
}

getFileAndCopy2Hdfs /nwbib/nwbib-spatial
getFileAndCopy2Hdfs /nwbib/nwbib
getFileAndCopy2Hdfs /rpb/rpb
