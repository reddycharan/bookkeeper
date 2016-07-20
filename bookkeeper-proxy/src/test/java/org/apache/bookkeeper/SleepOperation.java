package org.apache.bookkeeper;

public class SleepOperation extends AbstractOperation {

    private long sleepTimeInMilliSecs;

    public SleepOperation(int timeSlot, String threadId, long sleepTimeInMilliSecs) {
        this.timeSlot = timeSlot;
        this.threadId = threadId;
        this.sleepTimeInMilliSecs = sleepTimeInMilliSecs;
    }

    public static SleepOperation createSleepOperation(String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != Operation.SleepReq) {
            throw new IllegalArgumentException("Expected SleepReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        long sleepTimeInMilliSecs = Long.valueOf(operationParameters[3]);

        SleepOperation sleepOperation = new SleepOperation(timeSlot, threadId, sleepTimeInMilliSecs);

        return sleepOperation;
    }

    public long getSleepTimeInMilliSecs() {
        return sleepTimeInMilliSecs;
    }

    @Override
    public void setPrePerformSleepMsecs(int msecs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void preSetup(TestScenarioState state) {
        if (this.getSleepTimeInMilliSecs() >= BKPClientThread.timeoutDurationInSecs*1000) {
            // We always wait for timeoutDurationInSecs for an op to finish. Calculate how much additional
            // time we need to wait for since this op could be sleeping for a long time
            state.setAdditionalTimeoutWaitTime(
                Math.max(this.getSleepTimeInMilliSecs() - BKPClientThread.timeoutDurationInSecs*1000 + 1000, state.getAdditionalTimeoutWaitTime()));
        }
    };

    @Override
    public void perform(Object ctx) {
        try {
            Thread.sleep(sleepTimeInMilliSecs);
        } catch (InterruptedException e) {
            isOperationFailed = true;
            operationException = new InterruptedException(String.format(
                    "Operation at Timeslot: %d in ThreadId: %s has failed because of unexpected InterruptedException: %s",
                    getTimeSlot(), getThreadId(), e));
        }
    }
}
