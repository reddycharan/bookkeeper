#! /bin/bash

MANIFEST_DIR=`ls -ltr ~/installed/ | grep -i sfstore__main | tail -n 1`
CURRENT_STATUS=`cd ~/installed/$MANIFEST_DIR/bookkeeper/build; ./ant -q status`
#If sfstore is down, bring it back up; determined by its ./ant -q status check
if [[ ${CURRENT_STATUS} == *"Not Running"* ]]; then
    cd ~/installed/$MANIFEST_DIR/bookkeeper/build
    #Attempt to stop, in the even it is somehow in hampered state and deemed not running
    ./ant -q stop
    RESULT=`./ant -q start`
    if [[ $RESULT == *"BUILD SUCCESSFUL"* ]]; then
        echo "Successfully brought up bookkeeper via cron."
    else
		echo "Failed to start bookie at `date`"
    fi
fi
#If sfms is down, bring it back up; determined by its proc count of sfms (grep & actual proc)
if [[ `ps -ef | grep -i sfms | wc -l` -ne 2 ]]; then
    cd ~/installed/$MANIFEST_DIR/sayonara/build
    ./ant sfms.startSfstoreMetrics
fi

