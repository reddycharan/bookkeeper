package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class LedgerStatReqBKPOperation extends BKPOperationExtension {

    private long expectedSize;

    public LedgerStatReqBKPOperation(int timeSlot, String threadId, byte requestType, byte[] extentId,
            byte responseType, byte expectedReturnStatus, long expectedSize) {
        super(timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        this.expectedSize = expectedSize;
    }

    public long getExpectedSize() {
        return expectedSize;
    }

    public static LedgerStatReqBKPOperation createLedgerStatReqBKPOperation(String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerStatReq) {
            throw new IllegalArgumentException("Expected LedgerStatReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentUUIDBytes(operationParameters[3]);

        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);
        long expectedSize = Long.valueOf(operationParameters[5]);

        LedgerStatReqBKPOperation lsOperation = new LedgerStatReqBKPOperation(timeSlot, threadId, requestType, extentId,
                BKPConstants.LedgerStatResp, expectedReturnStatus, expectedSize);
        return lsOperation;
    }

    @Override
    public void receivePayloadAndVerify(SocketChannel clientSocketChannel) throws OperationException, IOException {
        if (getExpectedReturnStatus() == BKPConstants.SF_OK) {
            getLongFromResponseAndVerify(clientSocketChannel, expectedSize, "ExpectedSize");
        }
    }
}
