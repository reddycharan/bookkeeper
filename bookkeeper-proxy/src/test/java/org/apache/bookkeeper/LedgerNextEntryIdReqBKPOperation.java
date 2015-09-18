package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class LedgerNextEntryIdReqBKPOperation extends BKPOperationExtension {

    private int expectedNextEntryId;

    public LedgerNextEntryIdReqBKPOperation(int timeSlot, String threadId, byte requestType, byte[] extentId,
            byte responseType, byte expectedReturnStatus, int expectedNextEntryId) {
        super(timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        this.expectedNextEntryId = expectedNextEntryId;
        // TODO Auto-generated constructor stub
    }

    public int getExpectedNextEntryId() {
        return expectedNextEntryId;
    }

    public static LedgerNextEntryIdReqBKPOperation createLedgerNextEntryIdReqBKPOperation(String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerNextEntryIdReq) {
            throw new IllegalArgumentException("Expected LedgerNextEntryIdReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentUUIDBytes(operationParameters[3]);

        byte expectedReturnStatus = Byte.valueOf(operationParameters[4]);
        int expectedNextEntryId = -1;
        if (expectedReturnStatus == BKPConstants.SF_OK) {
            expectedNextEntryId = Integer.valueOf(operationParameters[5]);
        }
        LedgerNextEntryIdReqBKPOperation lneoperation = new LedgerNextEntryIdReqBKPOperation(timeSlot, threadId,
                requestType, extentId, BKPConstants.LedgerNextEntryIdResp, expectedReturnStatus, expectedNextEntryId);
        return lneoperation;
    }

    @Override
    public void receivePayloadAndVerify(SocketChannel clientSocketChannel) throws OperationException, IOException {
        if (getExpectedReturnStatus() == BKPConstants.SF_OK) {
            getIntFromResponseAndVerify(clientSocketChannel, expectedNextEntryId, "NextEntryId");
        }
    }
}
