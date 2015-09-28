package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class LedgerListGetReqBKPOperation extends BKPOperationExtension {

    public LedgerListGetReqBKPOperation(int timeSlot, String threadId, byte requestType, byte[] extentId,
            byte responseType, byte expectedReturnStatus) {
        super(timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        // TODO Auto-generated constructor stub
    }

    public static LedgerListGetReqBKPOperation createLedgerListGetReqBKPOperation(String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerListGetReq) {
            throw new IllegalArgumentException("Expected LedgerListGetReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentUUIDBytes(operationParameters[3]);

        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);
        LedgerListGetReqBKPOperation llgOperation = new LedgerListGetReqBKPOperation(timeSlot, threadId, requestType,
                extentId, BKPConstants.LedgerListGetResp, expectedReturnStatus);
        return llgOperation;
    }

}
