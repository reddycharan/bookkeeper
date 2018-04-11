#!/usr/bin/bash

# This script is used as a periodic liveness probe that will be executed at a pre-defined interval against each individual bookie.
# A zero-code exit is healthy; anything else is unhealthy, which will result in a container restart.
# We can add an arbitrary amount of checks here, so long as the various checks aren't too rigorous or time consuming.

cd /sfs/sfsbuild/bin
hostname=`hostname`

if [ `./bookkeeper shell listbookies -rw | grep -w $hostname | wc -l` -eq 1 ]; then
  # Host is in the read-write list of bookies.
  exit 0
elif [ `./bookkeeper shell listbookies -ro | grep -w $hostname | wc -l` -eq 1 ]; then
  # Host is identified in the read-only list of bookies.
  exit 0
else
# Found in neither read-write or read-only; flag probe as unhealthy.
  exit 1
fi
