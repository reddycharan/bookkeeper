package org.apache.bookkeeper;

public interface Operation {

    public String SPLITREGEX = "-";

    /**
     * The GeneralOperations Id should not overlap with the BKPOperations Id, which are defined in BKPConstants.java
     */
    public byte SleepReq = BKPConstants.LedgerFirstUnusedReq;
    public byte ZkServerPauseOpReq = BKPConstants.LedgerFirstUnusedReq + 1;

    public int getTimeSlot();

    public String getThreadId();

    public boolean isOperationFailed();

    public Exception getOperationException();

    public void preSetup(TestScenarioState state);

    public void setPrePerformSleepMsecs(int msecs);
    public void doPrePerformSleep() throws OperationException;
    public void perform(Object ctx);
}
