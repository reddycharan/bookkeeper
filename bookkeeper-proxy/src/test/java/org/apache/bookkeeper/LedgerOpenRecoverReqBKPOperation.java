package org.apache.bookkeeper;

public class LedgerOpenRecoverReqBKPOperation extends BKPOperationExtension {

    public LedgerOpenRecoverReqBKPOperation(short protocolVersion, int timeSlot, String threadId, byte requestType, 
            byte[] extentId, byte responseType, byte expectedReturnStatus) {
        super(protocolVersion, timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        // TODO Auto-generated constructor stub
    }

    public static LedgerOpenRecoverReqBKPOperation createLedgerOpenRecoverReqBKPOperation(short protocolVersion, String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerOpenRecoverReq) {
            throw new IllegalArgumentException("Expected LedgerOpenRecoverReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentIDBytes(operationParameters[3]);

        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);

        LedgerOpenRecoverReqBKPOperation lorOperation = new LedgerOpenRecoverReqBKPOperation(protocolVersion, timeSlot, threadId,
                requestType, extentId, BKPConstants.LedgerOpenRecoverResp, expectedReturnStatus);
        return lorOperation;
    }
}
