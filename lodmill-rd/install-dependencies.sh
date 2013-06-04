#!/bin/sh
cd ../..
git clone git://github.com/fsteeg/metafacture-core.git metafacture-core-dependency --branch regexp-empty-matches
cd metafacture-core-dependency
mvn clean install -DskipTests=true
