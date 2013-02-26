#!/bin/sh
export JAR=target/lodmill-rd-0.1.0-SNAPSHOT-jar-with-dependencies.jar
export RES=src/main/resources
cd ../lodmill-rd
mvn clean assembly:assembly -q --settings ../settings.xml
jar uf $JAR -C $RES metaflow-pipe.properties -C $RES metastream-encoders.properties
cp $JAR ../metatext/org.lobid.metatext/
