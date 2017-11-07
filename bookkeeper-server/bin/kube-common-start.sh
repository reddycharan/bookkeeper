#!/bin/bash

#Perform variable replacements that need to be done for both bookie & autorecovery containers.

set -e

#Accept a config file as a parameter and edit these configs
CONF=$1
sed -i.backup 's#.*useHostNameAsBookieID=.*#useHostNameAsBookieID=true#g' $CONF
sed -i.backup 's#.*zkLedgersRootPath=.*#zkLedgersRootPath=/sf-ledgers#g' $CONF
sed -i.backup 's#.*zkServers=.*#zkServers=sayonara1a-mnds2-1-prd.eng.sfdc.net:2181,sayonara1a-mnds2-2-prd.eng.sfdc.net:2181,sayonara1a-mnds2-3-prd.eng.sfdc.net:2181#g' $CONF
sed -i.backup 's#.*ledgerDirectories=.*#ledgerDirectories=/sfs/sfsdata/ledger#g' $CONF
sed -i.backup 's#.*journalDirectory=.*#journalDirectory=/sfs/sfsdata/journal#g' $CONF
sed -i.backup 's#.*bookiePort=.*#bookiePort=3181#g' $CONF
sed -i.backup 's#.*diskUsageThreshold=.*#diskUsageThreshold=0.85#g' $CONF
sed -i.backup 's#.*diskCheckInterval=.*#diskCheckInterval=60000#g' $CONF
