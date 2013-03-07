#!/bin/bash
# date: 201310128
# author: dr0i
# description: gets the metadata about the collections and the
# title resources of hbz-zvdd

java -classpath .:log4j-1.2.12.jar:harvester2.jar:xalan.jar ORG.oclc.oai.harvester2.app.RawWrite  \
        -metadataPrefix "marcxml" \
        -setSpec "collection" \
        -out "hbz_zvdd_collections_marc.xml" \
         http://www.digitalisiertedrucke.de/oai2d

java -classpath .:log4j-1.2.12.jar:harvester2.jar:xalan.jar ORG.oclc.oai.harvester2.app.RawWrite  \
        -metadataPrefix "marcxml" \
        -out "zvdd.xml" \
        http://www.digitalisiertedrucke.de/oai2d
