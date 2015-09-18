package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class LedgerReadCloseReqBKPOperation extends BKPOperationExtension {

    public LedgerReadCloseReqBKPOperation(int timeSlot, String threadId, byte requestType, byte[] extentId,
            byte responseType, byte expectedReturnStatus) {
        super(timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        // TODO Auto-generated constructor stub
    }

    public static LedgerReadCloseReqBKPOperation createLedgerReadCloseReqBKPOperation(String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerReadCloseReq) {
            throw new IllegalArgumentException("Expected LedgerReadCloseReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentUUIDBytes(operationParameters[3]);
        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);

        LedgerReadCloseReqBKPOperation lrcOperation = new LedgerReadCloseReqBKPOperation(timeSlot, threadId,
                requestType, extentId, BKPConstants.LedgerReadCloseResp, expectedReturnStatus);
        return lrcOperation;
    }
}
