package org.apache.bookkeeper;

public interface Operation {

    public String SPLITREGEX = "-";

    /**
     * The GeneralOperations Id should not overlap with the BKPOperations Id, which are defined in BKPConstants.java. So
     * we need to make sure that SleepReq Id - 91 is not used for any other constant in BKPConstants.java
     */
    public byte SleepReq = 91;

    public int getTimeSlot();

    public String getThreadId();

    public boolean isOperationFailed();

    public Exception getOperationException();

    public void preSetup(TestScenarioState state);

    public void perform(Object ctx);
}
