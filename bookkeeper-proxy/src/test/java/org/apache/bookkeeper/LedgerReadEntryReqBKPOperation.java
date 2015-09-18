package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

public class LedgerReadEntryReqBKPOperation extends BKPOperationExtension {

    private int fragmentID;
    private int size;
    private int expectedSize;

    public LedgerReadEntryReqBKPOperation(int timeSlot, String threadId, byte requestType, byte[] extentId,
            int fragmentID, int size, byte responseType, byte expectedReturnStatus, int expectedSize) {
        super(timeSlot, threadId, requestType, extentId, responseType, expectedReturnStatus);
        this.fragmentID = fragmentID;
        this.size = size;
        this.expectedSize = expectedSize;
    }

    public int getFragmentID() {
        return fragmentID;
    }

    public int getSize() {
        return size;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public static LedgerReadEntryReqBKPOperation createLedgerReadEntryReqBKPOperation(String operationDefinition) {
        String[] operationParameters = operationDefinition.split(SPLITREGEX);
        byte requestType = Byte.valueOf(operationParameters[2]);
        if (requestType != BKPConstants.LedgerReadEntryReq) {
            throw new IllegalArgumentException("Expected LedgerReadEntryReq in the Operation Definition");
        }

        int timeSlot = Integer.valueOf(operationParameters[0]);
        String threadId = operationParameters[1];
        byte[] extentId = TestScenarioState.getCurrentTestScenarioState().getExtentUUIDBytes(operationParameters[3]);
        int fragmentID = Integer.valueOf(operationParameters[4]);
        int size = Integer.valueOf(operationParameters[5]);
        byte expectedReturnStatus = Byte.valueOf(operationParameters[6]);
        int expectedSize = -1;
        if (expectedReturnStatus == BKPConstants.SF_OK) {
            expectedSize = Integer.valueOf(operationParameters[7]);
        }
        LedgerReadEntryReqBKPOperation lreOperation = new LedgerReadEntryReqBKPOperation(timeSlot, threadId,
                requestType, extentId, fragmentID, size, BKPConstants.LedgerReadEntryResp, expectedReturnStatus,
                expectedSize);
        return lreOperation;
    }

    @Override
    public void sendPayload(SocketChannel clientSocketChannel) throws IOException {
        putIntToRequest(clientSocketChannel, fragmentID);
        putIntToRequest(clientSocketChannel, size);
    }

    @Override
    public void receivePayloadAndVerify(SocketChannel clientSocketChannel) throws OperationException, IOException {
        if (getExpectedReturnStatus() == BKPConstants.SF_OK) {
            getIntFromResponseAndVerify(clientSocketChannel, expectedSize, "ExpectedSize");
            TestScenarioState currentTestScenario = TestScenarioState.getCurrentTestScenarioState();
            int fragmentIDToVerify;
            if (fragmentID == 0) {
                fragmentIDToVerify = currentTestScenario.getLastConfirmedFragmentId(getExtentId());
            } else {
                fragmentIDToVerify = fragmentID;
            }
            getEntryFromResponseAndVerify(clientSocketChannel,
                    currentTestScenario.getConfirmedFragment(getExtentId(), fragmentIDToVerify), "ExpectedByteArray");
        }
    }
}
