package org.apache.bookkeeper;

public class LedgerReadCloseReqBKPOperation extends BKPOperationExtension {

    public LedgerReadCloseReqBKPOperation(short protocolVersion, int timeSlot, String threadId, byte requestType,
            byte[] extentId, byte responseType, byte expectedReturnStatus) {
        super(protocolVersion, timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        // TODO Auto-generated constructor stub
    }

    public static LedgerReadCloseReqBKPOperation createLedgerReadCloseReqBKPOperation(short protocolVersion, String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerReadCloseReq) {
            throw new IllegalArgumentException("Expected LedgerReadCloseReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentIDBytes(operationParameters[3]);
        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);

        LedgerReadCloseReqBKPOperation lrcOperation = new LedgerReadCloseReqBKPOperation(protocolVersion, timeSlot, threadId,
                requestType, extentId, BKPConstants.LedgerReadCloseResp, expectedReturnStatus);
        return lrcOperation;
    }
}
