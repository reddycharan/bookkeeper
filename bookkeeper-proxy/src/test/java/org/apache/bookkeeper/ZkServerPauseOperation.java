package org.apache.bookkeeper;

import java.io.IOException;

public class ZkServerPauseOperation extends AbstractOperation {
	private int sleepMilliSecs;
    private BKProxyTestCase bkProxyTestCase;
    public ZkServerPauseOperation(int timeSlot, String threadId, int sleepMilliSecs, BKProxyTestCase bkProxyTestCase) {
        this.timeSlot = timeSlot;
        this.threadId = threadId;
        this.sleepMilliSecs = sleepMilliSecs;
        this.bkProxyTestCase = bkProxyTestCase;
    }

    public static ZkServerPauseOperation createZkServerPauseOperation(String operationDefinition, BKProxyTestCase bkProxyTestCase) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != Operation.ZkServerPauseOpReq) {
            throw new IllegalArgumentException("Expected ZkRestartReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        int sleepMilliSecs = Integer.valueOf(operationParameters[3]);

        ZkServerPauseOperation zkRestartOperation = new ZkServerPauseOperation(timeSlot, threadId, sleepMilliSecs, bkProxyTestCase);

        return zkRestartOperation;
    }

    @Override
    public void preSetup(TestScenarioState state) {
        // overriding preSetup since this op can block for a long time based on how long(controlled with 'sleepMilliSecs')
        // the zk sever should be kept in sleep state. We always wait for timeoutDurationInSecs for an op to finish. Calculate
        // how much additional time we need to wait for since the pause here could be long. Adding 2000 additional msecs for
        // the zk op to succeed after the server is brought online
        if (this.sleepMilliSecs >= BKPClientThread.timeoutDurationInSecs*1000) {
            state.setAdditionalTimeoutWaitTime(
                    Math.max((this.sleepMilliSecs - BKPClientThread.timeoutDurationInSecs*1000 + 2000), state.getAdditionalTimeoutWaitTime()));
        }
    }

    @Override
    public void perform(Object ctx) {
        try {
            bkProxyTestCase.pauseZkServers(sleepMilliSecs/1000);
        } catch (InterruptedException | IOException e) {
            isOperationFailed = true;
            operationException = new OperationException(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected Exception: %s",
                    getTimeSlot(), getThreadId(), e));
        }
    }
}
