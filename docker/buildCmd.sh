#!/usr/bin/env bash

set -e

mvn clean install -B -DskipTests
#Put it into the same structure as existing production directory.
cp bookkeeper-server/target/bookkeeper-server-4.7.0-SNAPSHOT.jar bookkeeper-server/
#CD to bookkeeper-server, and perform all operations relative to here.
tar -C bookkeeper-server/ -cf sfstore.tar bookkeeper-server-4.7.0-SNAPSHOT.jar ../LICENSE ../NOTICE ../README bin/ conf/ lib/
#Remove our previous copy.
rm bookkeeper-server/bookkeeper-server-4.7.0-SNAPSHOT.jar
#Zip it up & move it
gzip ./sfstore.tar
mv sfstore.tar.gz docker/
