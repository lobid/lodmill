# Add our application to the classpath:
export HADOOP_CLASSPATH=lobid-sample.jar

# Standalone operation is a s simple as:
# rm -rf lobid-sample-output
# bin/hadoop hadoop.sample.lobid.LobidToJsonLd lobid-sample.nt lobid-sample-output

# Pseudo-distributed operation is set up in:
# conf/core-site.xml, conf/hdfs-site.xml, conf/mapred-site.xml
# (see http://hadoop.apache.org/common/docs/r1.0.3/single_node_setup.html)

# clean up from previous run:
rm -rf ./lobid-sample-output
bin/hadoop fs -rm lobid-sample.nt
bin/hadoop fs -rmr lobid-sample-output

# initial setup, only once:
# ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
# cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys # for passphrase-less 'ssh localhost'
# bin/hadoop namenode -format # format a distributed fs
# bin/start-all.sh # start daemons, check http://localhost:50070/ and http://localhost:50030/

bin/hadoop fs -put lobid-sample.nt lobid-sample.nt # upload input data to hdfs
bin/hadoop hadoop.sample.lobid.LobidToJsonLd lobid-sample.nt lobid-sample-output # run job
bin/hadoop fs -get lobid-sample-output lobid-sample-output # download output data from hdfs
cat lobid-sample-output/part-r-00000 # show result

# bin/stop-all.sh
