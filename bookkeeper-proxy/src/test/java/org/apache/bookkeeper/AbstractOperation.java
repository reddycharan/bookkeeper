package org.apache.bookkeeper;

public abstract class AbstractOperation implements Operation {

    protected int timeSlot;
    protected String threadId;
    protected boolean isOperationFailed = false;
    protected Exception operationException;

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

    // for now, except SleepOperation no other operation needs this method. So it will be just dummy method.
    public void preSetup(TestScenarioState state) {
    };

    public static Operation build(String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        Operation operation = null;
        switch (requestType) {
        case Operation.SleepReq:
            operation = SleepOperation.createSleepOperation(operationDefinition);
            break;
        default:
            operation = BKPOperation.build(operationDefinition);
            break;
        }
        return operation;
    }
}
