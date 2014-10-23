# provides some variables and prerequisites

# get the newest code and build it
THIS_BASE="$(pwd)"

cd ../../../../..
git clone git://github.com/culturegraph/metafacture-core.git metafacture-core-dependency
cd metafacture-core-dependency
mvn clean install -DskipTests=true
cd ..
git clone https://github.com/hbz/metafacture-core.git
cd metafacture-core
mvn clean install -DskipTests=true

cd $THIS_BASE

cd ../../.. ; git pull
mvn clean assembly:assembly -DdescriptorId=jar-with-dependencies  -DskipTests=true -Dmysql.classifier=linux-amd64 -Dmysql.port=33061
cd $THIS_BASE
TARGET=../../../target/
cd $TARGET
JAR=$(ls lodmill-rd-*jar-with-dependencies.jar)
cp ../src/main/resources/morph_zdb-isil-file-pica2ld.xml ./
# TODO should be done with maven
cp ../src/main/resources/flux-commands.properties ./
jar uf $JAR flux-commands.properties
cd -

