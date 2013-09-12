#!/bin/sh
cd ../..
git clone https://github.com/culturegraph/metafacture-core.git metafacture-core-dependency
cd metafacture-core-dependency
git pull origin master
mvn clean install -DskipTests=true
