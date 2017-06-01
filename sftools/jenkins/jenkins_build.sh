#!/bin/bash
################################################################################
#
# Script to emulate the jenkins build output
#
################################################################################

set -o errexit
set -o pipefail

mvn -Pcodahale-metrics-provider clean install package assembly:single -DskipTests

SFSTORE_TAR=$(pwd)/deploy-sfstore.tar
rm ${SFSTORE_TAR} || true

tar cf ${SFSTORE_TAR} CHANGES.txt LICENSE NOTICE README
pushd bookkeeper-server
pushd target
tar rf ${SFSTORE_TAR} bookkeeper-server*.jar
popd
tar rf ${SFSTORE_TAR} bin conf lib
popd
bzip2 -fzq ${SFSTORE_TAR}
