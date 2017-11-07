#!/bin/bash

#Perform variable replacements that need to be done for both bookie & autorecovery containers.

set -e

sed -i.backup 's#.*useHostNameAsBookieID=.*#useHostNameAsBookieID=true#g' /sfs/sfsbuild/conf/bk_server.conf
sed -i.backup 's#.*zkLedgersRootPath=.*#zkLedgersRootPath=/sf-ledgers#g' /sfs/sfsbuild/conf/bk_server.conf
sed -i.backup 's#.*zkServers=.*#zkServers=zk-external.zookeeper.svc.cluster.local:2181#g' /sfs/sfsbuild/conf/bk_server.conf
sed -i.backup 's#.*ledgerDirectories=.*#ledgerDirectories=/sfs/sfsdata/ledger#g' /sfs/sfsbuild/conf/bk_server.conf
sed -i.backup 's#.*journalDirectory=.*#journalDirectory=/sfs/sfsdata/journal#g' /sfs/sfsbuild/conf/bk_server.conf
sed -i.backup 's#.*bookiePort=.*#bookiePort=3181#g' /sfs/sfsbuild/conf/bk_server.conf
sed -i.backup 's#.*diskUsageThreshold=.*#diskUsageThreshold=0.85#g' /sfs/sfsbuild/conf/bk_server.conf
sed -i.backup 's#.*diskCheckInterval=.*#diskCheckInterval=60000#g' /sfs/sfsbuild/conf/bk_server.conf
