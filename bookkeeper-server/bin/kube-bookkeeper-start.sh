#!/bin/bash

#Override bookkeeper-specific properties and then execute bookkeeper specific commands for initiating a new cluster in a k8s environment.

#Pass in the bk_server.conf as a parameter and edit its contents with common configs
/bin/bash /sfs/sfsbuild/bin/kube-common-start.sh /sfs/sfsbuild/conf/bk_server.conf
export BOOKIE_LOG_FILE_PATTERN=bookkeeper-bookie-$HOSTNAME-%d{yyyyMMddHH}.log
export BOOKIE_LOG_FORMAT=$HOSTNAME.sfstore.bookie
export BOOKIE_LOG_FILE=bookkeeper-bookie-$HOSTNAME.log
/sfs/sfsbuild/bin/bookkeeper shell initnewcluster
/sfs/sfsbuild/bin/bookkeeper shell bookieformat -d -nonInteractive
/sfs/sfsbuild/bin/bookkeeper bookie
