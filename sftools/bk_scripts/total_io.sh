#!/bin/sh

# This needs to be run in the directory where io_summary.out resides
# Verify the file with hostnames and ip addresses exist.
if [ ! -r ${PWD}/io_summary.out ]; then
    echo "ERROR:  'io_summary.out' file doesn't exist in current working directory."
    exit -1
fi

nodes=20

# Ledger Reads
reads=0
for lr in `grep -v nds io_summary.out | grep Ledger | tail -${nodes} | awk '{print $2}'`
do
    reads=`echo $reads + $lr | bc`
done

# Ledger Writes
writes=0
for lw in `grep -v nds io_summary.out | grep Ledger | tail -${nodes} | awk '{print $3}'`
do
    writes=`echo $writes + $lw | bc`
done

echo "Ledger Reads: $reads MB/s, Writes: $writes MB/s"


# Journal` Reads
reads=0
for jr in `grep -v nds io_summary.out | grep Journal | tail -${nodes} | awk '{print $2}'`
do
    reads=`echo $reads + $jr | bc`
done

# Journal Reads
writes=0
for jw in `grep -v nds io_summary.out | grep Journal | tail -${nodes} | awk '{print $3}'`
do
    writes=`echo $writes + $jw | bc`
done

echo "Journal Reads: $reads MB/s, Writes: $writes MB/s"


