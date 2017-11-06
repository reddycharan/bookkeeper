#!/bin/bash

#Override autorecovery-specific properties and then start-up autorecovery.

set -e
bash kube-common-start.sh
/sfs/sfsbuild/bin/bookkeeper autorecovery
