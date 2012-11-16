if [ $# -ne 1 ]
then
  echo "Usage: `basename $0` INPUT"
  exit 65
fi

export HADOOP=/opt/hadoop/hadoop
export HADOOP_CLASSPATH=../target/lodmill-0.1.0-SNAPSHOT-jar-with-dependencies.jar

# rm -rf ./json-ld-output
$HADOOP/bin/hadoop fs -rmr json-ld-output
$HADOOP/bin/hadoop org.lobid.lodmill.hadoop.NTriplesToJsonLd -D index.name=gnd-index -D index.type=json-ld-gnd -D cg.input.path=$1 -D cg.output.path=json-ld-output