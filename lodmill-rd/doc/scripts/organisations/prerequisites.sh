# provides some variables and prerequisites
JAR=lodmill-rd-1.1.0-SNAPSHOT-jar-with-dependencies.jar
TARGET=../../../target/
cp ../../../src/main/resources/morph_zdb-isil-file-pica2ld.xml ./
# TODO should be done with maven
cd $TARGET
cp ../src/main/resources/flux-commands.properties ./
jar uf $JAR flux-commands.properties
cd -

