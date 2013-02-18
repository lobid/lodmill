#!/bin/sh
mvn clean install
P2=../../lobid.github.com/p2
rm -rf $P2
cp -R org.lobid.metatext.p2/target/repository $P2
cd $P2/..
sh create-directory-listings.sh
git add -A .
# git commit
# git push origin master
