#!/bin/bash

# move it into hdfs:
export PATH="$PATH:/opt/hadoop/hadoop/bin/"; hadoop fs -rm hbzlod/lobid-organisations/lobid-organisationZDB.nt ; hadoop fs -copyFromLocal /files/open_data/closed/lobid-organisation/lobid-organisationZDB.nt hbzlod/lobid-organisations/ ; hadoop fs -rm hbzlod/lobid-organisations/allOrganisationsWithoutZDBIsil.nt ; hadoop fs -copyFromLocal /files/open_data/closed/lobid-organisation/allOrganisationsWithoutZDBIsil.nt hbzlod/lobid-organisations/;  hadoop fs -rm hbzlod/lobid-organisations/fundertype.nt; hadoop fs -copyFromLocal /files/open_data/closed/lobid-organisation/fundertype.nt hbzlod/lobid-organisations/ ; hadoop fs -rm hbzlod/lobid-organisations/stocksize.nt ; hadoop fs -copyFromLocal /files/open_data/closed/lobid-organisation/stocksize.nt hbzlod/lobid-organisations/
