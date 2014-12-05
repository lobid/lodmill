#!/bin/sh
cd ../..
git clone git://github.com/culturegraph/metafacture-core.git metafacture-core-dependency
cd metafacture-core-dependency
git pull
mvn clean install -DskipTests=true
cd ..
git clone https://github.com/hbz/metafacture-core.git
cd metafacture-core
git pull
mvn clean install -DskipTests=true
