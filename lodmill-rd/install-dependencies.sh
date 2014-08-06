#!/bin/sh
cd ../..
git clone git://github.com/culturegraph/metafacture-core.git metafacture-core-dependency
cd metafacture-core-dependency
mvn clean install -DskipTests=true
sudo apt-get install libaio-dev
