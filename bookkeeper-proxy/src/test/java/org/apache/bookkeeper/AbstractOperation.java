package org.apache.bookkeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOperation implements Operation {
    private final static Logger LOG = LoggerFactory.getLogger(AbstractOperation.class);
    protected int timeSlot;
    protected String threadId;
    protected boolean isOperationFailed = false;
    protected Exception operationException;
    protected int prePerformSleepMsecs;

    public int getTimeSlot() {
        return timeSlot;
    }

    public String getThreadId() {
        return threadId;
    }

    public boolean isOperationFailed() {
        return isOperationFailed;
    }

    public Exception getOperationException() {
        return operationException;
    }

    @Override
    public void preSetup(TestScenarioState state) {
        state.setAdditionalTimeoutWaitTime(
                Math.max(this.prePerformSleepMsecs, state.getAdditionalTimeoutWaitTime()));
    }

    public void setPrePerformSleepMsecs(int msecs) {
        this.prePerformSleepMsecs = msecs;
    }

    public void doPrePerformSleep() throws OperationException {
        try {
            if (this.prePerformSleepMsecs > 0) {
                LOG.info(threadId + ": Sleeping in doPrePerformSleep for " + prePerformSleepMsecs + " msecs");
                Thread.sleep(this.prePerformSleepMsecs);
            }
		} catch (InterruptedException e) {
            throw new OperationException(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected InterruptedException: %s",
                    getTimeSlot(), getThreadId(), e));
        }
    }

    public static Operation build(String operationDefinition, BKProxyTestCase bkProxyTestCase) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        Operation operation = null;
        switch (requestType) {
        case Operation.SleepReq:
            operation = SleepOperation.createSleepOperation(operationDefinition);
            break;
        case Operation.ZkServerPauseOpReq:
            operation = ZkServerPauseOperation.createZkServerPauseOperation(operationDefinition, bkProxyTestCase);
            break;
        default:
            operation = BKPOperation.build(operationDefinition);
            break;
        }
        return operation;
    }
}
