#!/bin/sh

#  This script will report a summary of IO metrics, once every 5 minutes, from
#  a remote collector of iostats.

# Verify the file with hostnames and ip addresses exist.
if [ ! -r ${PWD}/hosts ]; then
    echo "ERROR:  'hosts' file doesn't exist in current working directory."
    exit -1
fi

# The file format is expected to be the following format:
#
#   mnds1 : 10.225.199.38
#   mnds2 : 10.225.198.44
#   dnds3 : 10.225.199.13
#   dnds4 : 10.225.199.158
#   dnds5 : 10.225.197.68

ledger_dev=xvdb
journal_dev=xvdc

now=$(date +"%m_%d_%Y")
DIR=iotrends_$now
mkdir -p ${DIR}
cd ${DIR}

while (true)
do

  # Collect the io stats data
  echo "Collecting Stats from all store nodes"

  for host in `grep -i [md]nds ../hosts| awk '{print $3}'`
  do
      name=`ssh sfdc@${host} "uname -n|sed 's/gus-//'|sed -e 's/-.*$//'"`
      ssh sfdc@${host} "cd /sfs/sfslogs; tail -8 iostats.out > /tmp/io.out; chmod 664 /tmp/io.out"
      scp sfdc@${host}:/tmp/io.out ./io_${name}.out > /dev/null 2>&1
  done

  # Header
  cat io_mnds1.out | grep "Device" | sed '$!d' | awk '{print "host \t"$6"\t "$7"\t "$10}' >> summary.out

  rm -f io_summary.out

  echo "Processing per node stats"
  for i in `ls -rt1 io_*.out`
  do
     echo $i | sed 's/io_//' | sed -e 's/\..*$//' >> summary.out
     # Ledger
     grep ${ledger_dev} ${i} | awk '{print "Ledger\t"$6"\t"$7"\t"$10}' | sed '$!d' >> io_summary.out

     # Journal
     grep ${journal_dev} ${i} | awk '{print "Journal\t"$6"\t"$7"\t"$10}' | sed '$!d' >> io_summary.out
  done

  cat io_summary.out >> summary.out

  echo "Sleeping until next collection..."
  sleep 300

done
