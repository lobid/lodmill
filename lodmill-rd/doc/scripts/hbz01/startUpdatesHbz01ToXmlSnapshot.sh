#/bin/bash
# - Transform xml-MAB2-Update-Tar-Clobs sequentially. It is important to not do
# this in parallel when having multiple files, because older updates could
# overwrite newer ones.
# - xmls is splitted and lonely xml entities copied into fs. There, they reside as base for a new snapshotted update

FLUX=updatesHbz01ToXmlSnapshot.flux
UPDATE_FILES_LIST=toBeUpdateFilesXmlClobs.txt

JAR=$(basename $(ls ../../../target/lodmill-rd-*jar-with-dependencies.jar))
JAR=../../../target/$JAR

# update the jar with the flux commands from lodmill, overwritimg that of metafacture:
cd ../../../src/main/resources/
jar uf $JAR flux-commands.properties
cd -
# wait if load >1 , that is: wait until the machine has finished e.g. yesterdays updates
# ATTENTION: not safe enough - consider yesteradys yesterfdays files! A fifo is needed!
while [ "$(uptime |cut -d , -f 4|cut -d : -f 2 | cut -d . -f1 )" -ge 1 ]; do
  printf "."
  sleep 60
done

for i in $(cat $UPDATE_FILES_LIST); do
  echo "going to work on $i..."
  sed -i s#/home/data/demeter/alephxml/clobs/update/.*\"#$i\"#g $FLUX ;
  sed -i s#tmp.stats.csv.*#tmp.stats.csv.$(basename $i .tar.bz2)\"\)#g $FLUX;
  # wait when load >20
  while [ "$(uptime |cut -d , -f 4|cut -d : -f 2 | cut -d . -f1 )" -ge 20 ]; do
    printf "."
    sleep 60
  done
  java -classpath classes:$JAR:src/main/resources org.culturegraph.mf.Flux $FLUX
  # delete the first line in the update files list if everything was ok, else exit
  if [ $? -eq "0" ]; then
    echo "Status: success. Going to remove the line from $UPDATE_FILES_LIST"
    sed -i '1 d' $UPDATE_FILES_LIST
  else
    echo "Status: failed. Not removing the line from $UPDATE_FILES_LIST"
    exit
  fi
done
