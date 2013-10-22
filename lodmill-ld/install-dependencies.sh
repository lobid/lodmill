#!/bin/sh
cd ../..
git clone https://github.com/jsonld-java/jsonld-java.git jsonld-java-dependency
cd jsonld-java-dependency
git checkout 104d5b92cbbbfbb7d0e5d450ae669cb71f14adba
mvn clean install -DskipTests=true
