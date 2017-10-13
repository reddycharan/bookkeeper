#!/bin/sh

# Verify the file with hostnames and ip addresses exist.
if [ ! -r ${PWD}/hosts ]; then
    echo "ERROR:  hosts file doesn't exist in current working directory."
    exit -1
fi

# The file format is expected to be the following format:
#
#   mnds1 : 10.225.199.38
#   mnds2 : 10.225.198.44
#   dnds3 : 10.225.199.13
#   dnds4 : 10.225.199.158
#   dnds5 : 10.225.197.68

now=$(date +"%m_%d_%Y")

DIR=bkstats_${now}
mkdir -p ${DIR}
cd ${DIR}

for host in `grep [md]nds ../hosts| awk '{print $3}'`
do
   mkdir -p ${host}
   cd ${host}

   scp sfdc@${host}:/sfs/sfslogs/stats/* .
   cd ..
done
