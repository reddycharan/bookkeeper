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
#     APP : 10.225.196.160
#     APP : 10.225.197.185
#      DB : 10.225.197.241
#    QPID : 10.225.196.21
#    QPID : 10.225.199.123
#      LB : 10.225.198.130

now=$(date +"%m_%d_%Y")
DIR=iostats_$now
mkdir -p ${DIR}
cd ${DIR}

# Retrieve sdb latency output file from the DB hosts
if [ `grep DB ../hosts | wc -l` -ne 0 ]; then
    for host in `grep DB ../hosts| awk '{print $3}'`
    do
      scp sdb@${host}:/sdb/sdblogs/jims/latency.out ./latency_${host}.out
    done
fi

# Retrieve iostats output file from the store hosts
for host in `grep [md]nds ../hosts| awk '{print $3}'`
do
   echo "Copying io stats from ${host}..."
   scp sfdc@${host}:/sfs/sfslogs/iostats.out ./iostats_${host}.out 
done

