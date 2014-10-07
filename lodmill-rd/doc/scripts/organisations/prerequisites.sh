# provides some variables and prerequisites

# get the newest code and build it
cd ../../.. ; git pull;  mvn assembly:assembly; cd -

JAR=$(ls lodmill-rd-*jar-with-dependencies.jar)
TARGET=../../../target/
cp ../../../src/main/resources/morph_zdb-isil-file-pica2ld.xml ./
# TODO should be done with maven
cd $TARGET
cp ../src/main/resources/flux-commands.properties ./
jar uf $JAR flux-commands.properties
cd -

