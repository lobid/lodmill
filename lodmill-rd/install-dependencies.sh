#!/bin/sh

cat /etc/hosts

nslookup  localhost
sudo apt-get install dnsutils
dig localhost.localdomain
echo "done testing !"

cd ../..
git clone git://github.com/culturegraph/metafacture-core.git metafacture-core-dependency
cd metafacture-core-dependency
mvn clean install -DskipTests=true
cd ..
git clone https://github.com/hbz/metafacture-core.git
cd metafacture-core
mvn clean install -DskipTests=true
sudo apt-get install libaio-dev

