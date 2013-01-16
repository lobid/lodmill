#!/bin/sh
mvn clean assembly:assembly
scp target/lodmill-*-jar-with-dependencies.jar hydra1:.
