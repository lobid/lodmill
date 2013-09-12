#!/bin/sh
cd ../..
mvn clean assembly:assembly --settings ../settings.xml
scp target/lodmill-*-jar-with-dependencies.jar sol@weywot1.hbz-nrw.de:.
