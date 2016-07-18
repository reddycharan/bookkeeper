package org.apache.bookkeeper;

public class LedgerDeleteReqBKPOperation extends BKPOperationExtension {

    public LedgerDeleteReqBKPOperation(short protocolVersion, int timeSlot, String threadId, byte requestType, byte[] extentId,
            byte responseType, byte expectedReturnStatus) {
        super(protocolVersion, timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        // TODO Auto-generated constructor stub
    }

    public static LedgerDeleteReqBKPOperation createLedgerDeleteReqBKPOperation(short protocolVersion, String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerDeleteReq) {
            throw new IllegalArgumentException("Expected LedgerDeleteReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentIDBytes(operationParameters[3]);

        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);
        LedgerDeleteReqBKPOperation ldoperation = new LedgerDeleteReqBKPOperation(protocolVersion, timeSlot, threadId, requestType,
               extentId, BKPConstants.LedgerDeleteResp, expectedReturnStatus);
        return ldoperation;
    }

}
