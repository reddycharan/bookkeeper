package org.apache.bookkeeper;

public class LedgerOpenReadReqBKPOperation extends BKPOperationExtension {

    public LedgerOpenReadReqBKPOperation(short protocolVersion, int timeSlot, String threadId, byte requestType,
            byte[] extentId, byte responseType, byte expectedReturnStatus) {
        super(protocolVersion, timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        // TODO Auto-generated constructor stub
    }

    public static LedgerOpenReadReqBKPOperation createLedgerOpenReadReqBKPOperation(short protocolVersion, String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerOpenReadReq) {
            throw new IllegalArgumentException("Expected LedgerOpenReadReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentIDBytes(operationParameters[3]);

        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);

        LedgerOpenReadReqBKPOperation lorOperation = new LedgerOpenReadReqBKPOperation(protocolVersion, timeSlot, threadId, requestType,
                extentId, BKPConstants.LedgerOpenReadResp, expectedReturnStatus);
        return lorOperation;
    }
}
