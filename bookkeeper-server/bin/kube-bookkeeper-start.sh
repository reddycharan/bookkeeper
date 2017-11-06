#!/bin/bash

#Override bookkeeper-specific properties and then execute bookkeeper specific commands for initiating a new cluster in a k8s environment.

set -e
bash kube-common-start.sh
/sfs/sfsbuild/bin/bookkeeper shell initNewCluster -nonInteractive
/sfs/sfsbuild/bin/bookkeeper shell bookieformat -d -nonInteractive
/sfs/sfsbuild/bin/bookkeeper bookie
