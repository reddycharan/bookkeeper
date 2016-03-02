#!/bin/sh -e

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

for host in `grep -e [md]nds hosts| awk '{print $3}'`
do
echo $i
   ssh sfdc@${host} "kill -9 \`/sbin/pidof iostat\`"
done

