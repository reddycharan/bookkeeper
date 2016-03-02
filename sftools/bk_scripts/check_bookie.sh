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

userid=sfdc

echo "Checking for running bookies"
status=0
for host in `grep -i [md]nds hosts| awk '{print $3}'`
do
   echo "${host} : \c"

   count=`ssh ${userid}@${host} "pgrep -f bookkeeper" | wc -l`
   if [ "$count" == "0" ]; then
       echo "bookkeeper doesn't appear to be running..."
       status=$((status + 1))
   else
       count=`ssh ${userid}@${host} "grep 'Register shutdown hook successfully' /sfs/sfslogs/book*log*" | wc -l `
       if [ "$count" == "0" ]; then
           echo "bookkeeper doesn't appear to be running..."
           status=$((status + 1))
       fi
   fi
done

exit $status
