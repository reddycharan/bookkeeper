#! /bin/bash

cd /home/sfstore/current/sfstorage/bookkeeper/build
CURRENT_STATUS=`./ant -q status`
#If sfstore is down, bring it back up; determined by its ./ant -q status check
if [[ ${CURRENT_STATUS} == *"Not Running"* ]]; then
    #Starting implicitly stops both services without exiting.
    RESULT=`./ant -q start`
    if [[ $RESULT == *"BUILD SUCCESSFUL"* ]]; then
        echo "Successfully brought up bookkeeper via cron."
    else
        echo "Failed to start bookie at `date`"
    fi
fi
#If sfms is down, bring it back up; determined by its proc count of sfms (grep & actual proc)
if [[ `ps -ef | grep -i [s]fms | wc -l` -lt 2 ]]; then
    cd /home/sfstore/current/sfstorage/sayonara/build
    ./ant sfms.startSfstoreMetrics
fi

