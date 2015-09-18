package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class LedgerCreateReqBKPOperation extends BKPOperationExtension {

    public LedgerCreateReqBKPOperation(int timeSlot, String threadId, byte requestType, byte[] extentId,
            byte responseType, byte expectedReturnStatus) {
        super(timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
    }

    public static LedgerCreateReqBKPOperation createLedgerCreateReqBKPOperation(String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerCreateReq) {
            throw new IllegalArgumentException("Expected LedgerCreateReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentUUIDBytes(operationParameters[3]);

        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);

        LedgerCreateReqBKPOperation lcrOperation = new LedgerCreateReqBKPOperation(timeSlot, threadId, requestType,
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
