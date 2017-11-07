#!/bin/bash

#Override autorecovery-specific properties and then start-up autorecovery.

#Pass in the bk_replication.conf as a parameter and edit its contents with common configs
/bin/bash /sfs/sfsbuild/bin/kube-common-start.sh /sfs/sfsbuild/conf/bk_replication.conf
export BOOKIE_LOG_FILE_PATTERN=bookkeeper-autorecovery-$HOSTNAME-%d{yyyyMMddHH}.log
export BOOKIE_LOG_FORMAT=$HOSTNAME.sfstore.autorecovery
export BOOKIE_LOG_FILE=bookkeeper-autorecovery-$HOSTNAME.log
/sfs/sfsbuild/bin/bookkeeper autorecovery
