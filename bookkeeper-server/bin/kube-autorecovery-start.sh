#!/bin/bash

#Override autorecovery-specific properties and then start-up autorecovery.

bash ./kube-common-start.sh
export BOOKIE_LOG_FILE_PATTERN=bookkeeper-autorecovery-$HOSTNAME-%d{yyyyMMddHH}.log
export BOOKIE_LOG_FORMAT=$HOSTNAME.sfstore.autorecovery
export BOOKIE_LOG_FILE=bookkeeper-autorecovery-$HOSTNAME.log
/sfs/sfsbuild/bin/bookkeeper autorecovery
