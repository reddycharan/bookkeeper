package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class LedgerWriteCloseReqBKPOperation extends BKPOperationExtension {

    public LedgerWriteCloseReqBKPOperation(int timeSlot, String threadId, byte requestType, byte[] extentId,
            byte responseType, byte expectedReturnStatus) {
        super(timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        // TODO Auto-generated constructor stub
    }

    public static LedgerWriteCloseReqBKPOperation createLedgerWriteCloseReqBKPOperation(String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerWriteCloseReq) {
            throw new IllegalArgumentException("Expected LedgerWriteCloseReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentUUIDBytes(operationParameters[3]);
        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);
        LedgerWriteCloseReqBKPOperation lwcOperation = new LedgerWriteCloseReqBKPOperation(timeSlot, threadId,
                requestType, extentId, BKPConstants.LedgerWriteCloseResp, expectedReturnStatus);

        return lwcOperation;
    }
}
