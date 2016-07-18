package org.apache.bookkeeper;

public class LedgerCreateReqBKPOperation extends BKPOperationExtension {

    public LedgerCreateReqBKPOperation(short protocolVersion, int timeSlot, String threadId, byte requestType, byte[] extentId,
            byte responseType, byte expectedReturnStatus) {
        super(protocolVersion, timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
    }

    public static LedgerCreateReqBKPOperation createLedgerCreateReqBKPOperation(short protocolVersion, String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerCreateReq) {
            throw new IllegalArgumentException("Expected LedgerCreateReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentIDBytes(operationParameters[3]);

        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);

        LedgerCreateReqBKPOperation lcrOperation = new LedgerCreateReqBKPOperation(protocolVersion, timeSlot, threadId, requestType,
                 extentId, BKPConstants.LedgerCreateResp, expectedReturnStatus);

        return lcrOperation;
    }

    @Override
    public void catalogBookKeeping() {
        if (getExpectedReturnStatus() == BKPConstants.SF_OK) {
            TestScenarioState.getCurrentTestScenarioState().newExtentCreated(getExtentId());
        }
    }
}
